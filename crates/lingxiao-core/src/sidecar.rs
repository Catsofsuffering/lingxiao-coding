use crate::process::ProcessRegistry;
use lingxiao_tool_host_protocol::{
    PermissionLease, ResourceUsage, SidecarError, SidecarErrorCode, SidecarRequest, SidecarResponse,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

const SIDE_CAR_OUTPUT_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct SidecarCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct SidecarInvocation {
    pub command: SidecarCommand,
    pub request: SidecarRequest,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SidecarExecution {
    pub response: SidecarResponse,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub struct CancelHandle {
    token_id: String,
    flag: Arc<AtomicBool>,
}

impl CancelHandle {
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
pub struct SidecarScheduler {
    cancel_tokens: Mutex<HashMap<String, Arc<AtomicBool>>>,
    process_registry: Option<ProcessRegistry>,
}

impl SidecarScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_process_registry(process_registry: ProcessRegistry) -> Self {
        Self {
            cancel_tokens: Mutex::new(HashMap::new()),
            process_registry: Some(process_registry),
        }
    }

    pub fn register_cancel_token(&self, token_id: impl Into<String>) -> CancelHandle {
        let token_id = token_id.into();
        let flag = Arc::new(AtomicBool::new(false));
        self.cancel_tokens
            .lock()
            .unwrap()
            .insert(token_id.clone(), flag.clone());
        CancelHandle { token_id, flag }
    }

    pub fn cancel(&self, token_id: &str) -> bool {
        let Some(flag) = self.cancel_tokens.lock().unwrap().get(token_id).cloned() else {
            return false;
        };
        flag.store(true, Ordering::SeqCst);
        true
    }

    pub fn unregister_cancel_token(&self, token_id: &str) -> bool {
        self.cancel_tokens
            .lock()
            .unwrap()
            .remove(token_id)
            .is_some()
    }

    pub fn validate_permission_lease(
        &self,
        lease: &PermissionLease,
        target: &Path,
        now_ms: i64,
    ) -> Result<(), SidecarErrorCode> {
        if now_ms > lease.expires_at {
            return Err(SidecarErrorCode::PermissionDenied);
        }
        if !path_within_scope(target, Path::new(&lease.scope)) {
            return Err(SidecarErrorCode::FileWriteDenied);
        }
        Ok(())
    }

    pub fn execute(&self, invocation: SidecarInvocation) -> Result<SidecarExecution, SidecarError> {
        let cancel = self.register_cancel_token(invocation.request.cancel_token.token_id.clone());
        let mut child = Command::new(&invocation.command.program)
            .args(&invocation.command.args)
            .current_dir(
                invocation
                    .command
                    .cwd
                    .as_deref()
                    .unwrap_or_else(|| Path::new(".")),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| sidecar_error(&invocation.request, SidecarErrorCode::TransportError, e))?;
        let stdout_drain = child.stdout.take().map(spawn_output_drain);
        let stderr_drain = child.stderr.take().map(spawn_output_drain);
        let process_id = format!("sidecar:{}", invocation.request.tool_call_id);
        if let Some(registry) = &self.process_registry {
            let _ = registry.register(
                &process_id,
                child.id(),
                "sidecar",
                &invocation.request.tool_call_id,
                invocation.command.program.display().to_string(),
            );
        }

        if let Some(stdin) = child.stdin.as_mut() {
            serde_json::to_writer(&mut *stdin, &invocation.request).map_err(|e| {
                sidecar_error(&invocation.request, SidecarErrorCode::ProtocolError, e)
            })?;
            stdin.write_all(b"\n").map_err(|e| {
                sidecar_error(&invocation.request, SidecarErrorCode::TransportError, e)
            })?;
        }
        drop(child.stdin.take());

        let started = Instant::now();
        let status = loop {
            if cancel.is_cancelled() {
                let _ = child.kill();
                let _ = child.wait();
                let stderr = collect_output(stderr_drain);
                let _ = collect_output(stdout_drain);
                if let Some(registry) = &self.process_registry {
                    let _ = registry.mark_failed(&process_id, "sidecar cancelled");
                }
                self.unregister_cancel_token(cancel.token_id());
                return Err(SidecarError {
                    request_id: invocation.request.request_id,
                    code: SidecarErrorCode::Cancelled,
                    message: "sidecar cancelled".into(),
                    details: Some(stderr),
                    usage: ResourceUsage::default(),
                });
            }
            if started.elapsed() > Duration::from_millis(invocation.timeout_ms) {
                let _ = child.kill();
                let _ = child.wait();
                let stderr = collect_output(stderr_drain);
                let _ = collect_output(stdout_drain);
                if let Some(registry) = &self.process_registry {
                    let _ = registry.mark_failed(&process_id, "sidecar timeout");
                }
                self.unregister_cancel_token(cancel.token_id());
                return Err(SidecarError {
                    request_id: invocation.request.request_id,
                    code: SidecarErrorCode::Timeout,
                    message: "sidecar timeout".into(),
                    details: Some(stderr),
                    usage: ResourceUsage::default(),
                });
            }
            let remaining = Duration::from_millis(invocation.timeout_ms)
                .saturating_sub(started.elapsed())
                .min(Duration::from_millis(10));
            if let Some(status) = child.wait_timeout(remaining).map_err(|e| {
                sidecar_error(&invocation.request, SidecarErrorCode::TransportError, e)
            })? {
                break status;
            }
        };
        let stdout_bytes = collect_output(stdout_drain);
        let stderr_bytes = collect_output(stderr_drain);
        let stdout = String::from_utf8_lossy(&stdout_bytes).to_string();
        let stderr = String::from_utf8_lossy(&stderr_bytes).to_string();
        if !status.success() {
            if let Some(registry) = &self.process_registry {
                let _ = registry.mark_failed(&process_id, "sidecar crashed");
            }
            self.unregister_cancel_token(cancel.token_id());
            return Err(SidecarError {
                request_id: invocation.request.request_id,
                code: SidecarErrorCode::Crash,
                message: format!("sidecar exited with status {status}"),
                details: Some(stderr_bytes),
                usage: ResourceUsage::default(),
            });
        }

        let response = parse_sidecar_response(&stdout).map_err(|e| SidecarError {
            request_id: invocation.request.request_id,
            code: SidecarErrorCode::ProtocolError,
            message: e.to_string(),
            details: Some(stdout_bytes),
            usage: ResourceUsage::default(),
        })?;
        if let Some(registry) = &self.process_registry {
            let _ = registry.complete(&process_id, status.code());
        }
        self.unregister_cancel_token(cancel.token_id());
        Ok(SidecarExecution {
            response,
            stdout,
            stderr,
        })
    }
}

struct OutputDrain {
    buffer: Arc<Mutex<Vec<u8>>>,
    handle: JoinHandle<()>,
}

fn spawn_output_drain<R>(mut reader: R) -> OutputDrain
where
    R: Read + Send + 'static,
{
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let thread_buffer = buffer.clone();
    let handle = thread::spawn(move || {
        let mut chunk = [0_u8; 8192];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => append_bounded(&thread_buffer, &chunk[..n]),
                Err(_) => break,
            }
        }
    });
    OutputDrain { buffer, handle }
}

fn append_bounded(buffer: &Arc<Mutex<Vec<u8>>>, bytes: &[u8]) {
    let mut guard = buffer.lock().unwrap();
    guard.extend_from_slice(bytes);
    if guard.len() > SIDE_CAR_OUTPUT_LIMIT {
        let overflow = guard.len() - SIDE_CAR_OUTPUT_LIMIT;
        guard.drain(..overflow);
    }
}

fn collect_output(drain: Option<OutputDrain>) -> Vec<u8> {
    let Some(drain) = drain else {
        return Vec::new();
    };
    let _ = drain.handle.join();
    let output = drain.buffer.lock().unwrap().clone();
    output
}

fn path_within_scope(target: &Path, scope: &Path) -> bool {
    let target = normalize_path(target);
    let scope = normalize_path(scope);
    target == scope || target.strip_prefix(scope).is_ok()
}

fn normalize_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    })
}

fn parse_sidecar_response(stdout: &str) -> Result<SidecarResponse, serde_json::Error> {
    let mut final_stream = None;
    let mut last_error = None;
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        match serde_json::from_str::<SidecarResponse>(line) {
            Ok(SidecarResponse::Completed(completed)) => {
                return Ok(SidecarResponse::Completed(completed));
            }
            Ok(SidecarResponse::Error(error)) => {
                return Ok(SidecarResponse::Error(error));
            }
            Ok(SidecarResponse::Stream(chunk)) if chunk.is_final => {
                final_stream = Some(SidecarResponse::Stream(chunk));
            }
            Ok(SidecarResponse::Stream(_)) | Ok(SidecarResponse::Progress(_)) => {}
            Err(error) => {
                if serde_json::from_str::<lingxiao_tool_host_protocol::SidecarHeartbeat>(line)
                    .is_err()
                {
                    last_error = Some(error);
                }
            }
        }
    }
    if let Some(response) = final_stream {
        return Ok(response);
    }
    match last_error {
        Some(error) => Err(error),
        None => serde_json::from_str(""),
    }
}

fn sidecar_error(
    request: &SidecarRequest,
    code: SidecarErrorCode,
    err: impl std::fmt::Display,
) -> SidecarError {
    SidecarError {
        request_id: request.request_id.clone(),
        code,
        message: err.to_string(),
        details: None,
        usage: ResourceUsage::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lingxiao_tool_host_protocol::{
        CancelToken, HeartbeatHealth, ResourceBudget, SidecarCompleted, SidecarHeartbeat,
        SidecarProgress, SidecarStreamChunk,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_sidecar_scheduler_creation() {
        let scheduler = SidecarScheduler::new();
        let handle = scheduler.register_cancel_token("ct-create");
        assert_eq!(handle.token_id(), "ct-create");
        assert!(!handle.is_cancelled());
        assert!(scheduler.cancel("ct-create"));
        assert!(handle.is_cancelled());
    }

    /// Compile-time smoke test: all protocol types are usable from core.
    #[test]
    fn test_protocol_types_smoke() {
        let req = sample_request("req_001", "tc_001");
        let json = serde_json::to_string(&req).unwrap();
        let _restored: SidecarRequest = serde_json::from_str(&json).unwrap();

        let resp = SidecarResponse::Completed(SidecarCompleted {
            request_id: "req_001".into(),
            result: br#"{"ok":true}"#.to_vec(),
            result_shape: "json".into(),
            duration_ms: 500,
            usage: ResourceUsage::default(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        let _restored: SidecarResponse = serde_json::from_str(&json).unwrap();

        let _resp = SidecarResponse::Stream(SidecarStreamChunk {
            request_id: "req_001".into(),
            sequence: 1,
            data: b"chunk".to_vec(),
            is_final: false,
            usage: None,
        });
        let _resp = SidecarResponse::Progress(SidecarProgress {
            request_id: "req_001".into(),
            percent: Some(0.5),
            message: Some("halfway".into()),
            usage: ResourceUsage::default(),
        });
        let _hb = SidecarHeartbeat {
            request_id: "req_001".into(),
            usage: ResourceUsage::default(),
            health: HeartbeatHealth::Healthy,
        };
    }

    #[test]
    fn test_permission_lease_scope_and_expiry() {
        let scheduler = SidecarScheduler::new();
        let root = std::env::temp_dir().join("lingxiao_lease_scope");
        let lease = PermissionLease {
            scope: root.to_string_lossy().to_string(),
            generation: 1,
            expires_at: now_ms() + 10_000,
        };
        assert!(scheduler
            .validate_permission_lease(&lease, &root.join("ok.txt"), now_ms())
            .is_ok());
        assert!(matches!(
            scheduler.validate_permission_lease(
                &lease,
                &std::env::temp_dir().join("outside.txt"),
                now_ms()
            ),
            Err(SidecarErrorCode::FileWriteDenied)
        ));

        let expired = PermissionLease {
            expires_at: now_ms() - 1,
            ..lease
        };
        assert!(matches!(
            scheduler.validate_permission_lease(&expired, &root.join("ok.txt"), now_ms()),
            Err(SidecarErrorCode::PermissionDenied)
        ));
    }

    #[test]
    fn test_permission_lease_scope_uses_path_boundary_not_prefix() {
        let scheduler = SidecarScheduler::new();
        let root = std::env::temp_dir().join("lingxiao_scope_boundary");
        let sibling = std::env::temp_dir().join("lingxiao_scope_boundary_safe");
        let lease = PermissionLease {
            scope: root.to_string_lossy().to_string(),
            generation: 1,
            expires_at: now_ms() + 10_000,
        };

        assert!(matches!(
            scheduler.validate_permission_lease(&lease, &sibling.join("escape.txt"), now_ms()),
            Err(SidecarErrorCode::FileWriteDenied)
        ));
    }

    #[test]
    fn test_parse_sidecar_frames_skips_progress_heartbeat_and_returns_completed() {
        let progress = SidecarResponse::Progress(SidecarProgress {
            request_id: "req_frames".into(),
            percent: Some(0.5),
            message: Some("halfway".into()),
            usage: ResourceUsage::default(),
        });
        let heartbeat = SidecarHeartbeat {
            request_id: "req_frames".into(),
            usage: ResourceUsage::default(),
            health: HeartbeatHealth::Healthy,
        };
        let completed = SidecarResponse::Completed(SidecarCompleted {
            request_id: "req_frames".into(),
            result: b"done".to_vec(),
            result_shape: "text".into(),
            duration_ms: 10,
            usage: ResourceUsage::default(),
        });
        let stdout = format!(
            "{}\n{}\n{}\n",
            serde_json::to_string(&progress).unwrap(),
            serde_json::to_string(&heartbeat).unwrap(),
            serde_json::to_string(&completed).unwrap()
        );

        let parsed = parse_sidecar_response(&stdout).unwrap();
        assert!(matches!(
            parsed,
            SidecarResponse::Completed(done) if done.result == b"done"
        ));
    }

    #[test]
    fn test_stdio_sidecar_completed_roundtrip() {
        let scheduler = SidecarScheduler::new();
        let script = write_echo_sidecar();
        let request = sample_request("req_echo", "tc_echo");
        let execution = scheduler
            .execute(SidecarInvocation {
                command: SidecarCommand {
                    program: powershell(),
                    args: vec![
                        "-NoProfile".into(),
                        "-ExecutionPolicy".into(),
                        "Bypass".into(),
                        "-File".into(),
                        script.to_string_lossy().to_string(),
                    ],
                    cwd: None,
                },
                request,
                timeout_ms: 5_000,
            })
            .unwrap();
        match execution.response {
            SidecarResponse::Completed(done) => {
                assert_eq!(done.request_id, "req_echo");
                assert_eq!(done.result_shape, "json");
            }
            other => panic!("expected completed response, got {other:?}"),
        }
    }

    #[test]
    fn test_stdio_sidecar_timeout_maps_to_error() {
        let scheduler = SidecarScheduler::new();
        let script = write_sleep_sidecar();
        let request = sample_request("req_timeout", "tc_timeout");
        let err = scheduler
            .execute(SidecarInvocation {
                command: SidecarCommand {
                    program: powershell(),
                    args: vec![
                        "-NoProfile".into(),
                        "-ExecutionPolicy".into(),
                        "Bypass".into(),
                        "-File".into(),
                        script.to_string_lossy().to_string(),
                    ],
                    cwd: None,
                },
                request,
                timeout_ms: 50,
            })
            .unwrap_err();
        assert!(matches!(err.code, SidecarErrorCode::Timeout));
    }

    fn sample_request(request_id: &str, tool_call_id: &str) -> SidecarRequest {
        SidecarRequest {
            request_id: request_id.into(),
            session_id: "ses_abc".into(),
            task_id: Some("task_42".into()),
            agent_id: "agent_1".into(),
            tool_call_id: tool_call_id.into(),
            tool_name: "test".into(),
            args: br#"{"ok":true}"#.to_vec(),
            capabilities: vec!["test".into()],
            deadline: now_ms() + 60_000,
            resource_budget: ResourceBudget {
                max_runtime_ms: 120_000,
                max_memory_mb: 512,
                max_cpu_ms: 120_000,
                max_network_bytes: 10_000_000,
                max_file_write_bytes: 50_000_000,
            },
            cancel_token: CancelToken {
                token_id: format!("ct_{request_id}"),
            },
            permission_lease: None,
        }
    }

    fn write_echo_sidecar() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lingxiao_sidecar_{}", now_ms()));
        fs::create_dir_all(&dir).unwrap();
        let script = dir.join("echo_sidecar.ps1");
        fs::write(
            &script,
            r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
$response = @{
  Completed = @{
    request_id = $req.request_id
    result = [System.Text.Encoding]::UTF8.GetBytes('{"ok":true}')
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
        .unwrap();
        script
    }

    fn write_sleep_sidecar() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lingxiao_sidecar_sleep_{}", now_ms()));
        fs::create_dir_all(&dir).unwrap();
        let script = dir.join("sleep_sidecar.ps1");
        fs::write(
            &script,
            r#"
Start-Sleep -Seconds 3
"#,
        )
        .unwrap();
        script
    }

    fn powershell() -> PathBuf {
        resolve_powershell()
    }

    // Resolve an absolute powershell.exe path. Rust's `Command` on Windows does
    // not perform PATHEXT resolution, so a bare `"powershell"` fails to spawn in
    // some shells. Anchor to the system root when available.
    fn resolve_powershell() -> PathBuf {
        for key in ["SystemRoot", "windir", "SYSTEMROOT", "WINDIR"] {
            if let Ok(root) = std::env::var(key) {
                let candidate = Path::new(&root)
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
