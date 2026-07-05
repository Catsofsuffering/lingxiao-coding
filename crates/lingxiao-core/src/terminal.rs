use crate::process::ProcessRegistry;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug, Clone)]
pub struct TerminalCreateOptions {
    pub terminal_id: String,
    pub shell: Option<String>,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalCreated {
    pub terminal_id: String,
    pub pid: u32,
    pub shell: String,
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRead {
    pub terminal_id: String,
    pub stdout: String,
    pub stderr: String,
    pub running: bool,
    pub exit_code: Option<i32>,
}

struct TerminalSession {
    child: Child,
    stdin: ChildStdin,
    stdout: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
}

#[derive(Clone)]
pub struct TerminalManager {
    sessions: Arc<Mutex<HashMap<String, TerminalSession>>>,
    process_registry: ProcessRegistry,
}

impl TerminalManager {
    pub fn new(process_registry: ProcessRegistry) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            process_registry,
        }
    }

    pub fn create(&self, options: TerminalCreateOptions) -> Result<TerminalCreated, String> {
        let (shell, args) = shell_command(options.shell, options.args);
        let mut command = Command::new(&shell);
        command
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(cwd) = options.cwd.as_deref() {
            command.current_dir(cwd);
        }

        let mut child = command
            .spawn()
            .map_err(|error| format!("terminal spawn failed: {error}"))?;
        let pid = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "terminal stdin unavailable".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "terminal stdout unavailable".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "terminal stderr unavailable".to_string())?;

        let stdout_buffer = Arc::new(Mutex::new(Vec::new()));
        let stderr_buffer = Arc::new(Mutex::new(Vec::new()));
        spawn_reader(stdout, stdout_buffer.clone());
        spawn_reader(stderr, stderr_buffer.clone());

        if let Err(error) = self.process_registry.register(
            format!("terminal:{}", options.terminal_id),
            pid,
            "terminal",
            options.terminal_id.clone(),
            shell.clone(),
        ) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("terminal process registration failed: {error}"));
        }

        let cwd = options
            .cwd
            .as_deref()
            .map(|path| path.display().to_string());
        self.sessions.lock().unwrap().insert(
            options.terminal_id.clone(),
            TerminalSession {
                child,
                stdin,
                stdout: stdout_buffer,
                stderr: stderr_buffer,
            },
        );

        Ok(TerminalCreated {
            terminal_id: options.terminal_id,
            pid,
            shell,
            cwd,
        })
    }

    pub fn send(&self, terminal_id: &str, input: &str) -> Result<usize, String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get_mut(terminal_id)
            .ok_or_else(|| format!("terminal session not live: {terminal_id}"))?;
        if let Some(status) = session
            .child
            .try_wait()
            .map_err(|error| format!("terminal status failed: {error}"))?
        {
            let _ = self
                .process_registry
                .complete(&format!("terminal:{terminal_id}"), status.code());
            return Err(format!("terminal session exited: {terminal_id}"));
        }
        session
            .stdin
            .write_all(input.as_bytes())
            .map_err(|error| format!("terminal write failed: {error}"))?;
        session
            .stdin
            .flush()
            .map_err(|error| format!("terminal flush failed: {error}"))?;
        Ok(input.len())
    }

    pub fn read(&self, terminal_id: &str, max_bytes: usize) -> Result<TerminalRead, String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get_mut(terminal_id)
            .ok_or_else(|| format!("terminal session not live: {terminal_id}"))?;
        let status = session
            .child
            .try_wait()
            .map_err(|error| format!("terminal status failed: {error}"))?;
        if let Some(status) = status {
            let _ = self
                .process_registry
                .complete(&format!("terminal:{terminal_id}"), status.code());
        }
        let stdout = {
            let guard = session.stdout.lock().unwrap();
            tail_utf8(&guard, max_bytes)
        };
        let stderr = {
            let guard = session.stderr.lock().unwrap();
            tail_utf8(&guard, max_bytes)
        };
        Ok(TerminalRead {
            terminal_id: terminal_id.to_string(),
            stdout,
            stderr,
            running: status.is_none(),
            exit_code: status.and_then(|status| status.code()),
        })
    }

    pub fn kill(&self, terminal_id: &str) -> Result<Option<i32>, String> {
        let mut sessions = self.sessions.lock().unwrap();
        let mut session = sessions
            .remove(terminal_id)
            .ok_or_else(|| format!("terminal session not live: {terminal_id}"))?;
        let status = session
            .child
            .try_wait()
            .map_err(|error| format!("terminal status failed: {error}"))?;
        let exit_code = match status {
            Some(status) => status.code(),
            None => {
                session
                    .child
                    .kill()
                    .map_err(|error| format!("terminal kill failed: {error}"))?;
                session
                    .child
                    .wait()
                    .map_err(|error| format!("terminal wait failed: {error}"))?
                    .code()
            }
        };
        let _ = self
            .process_registry
            .complete(&format!("terminal:{terminal_id}"), exit_code);
        Ok(exit_code)
    }

    pub fn live_session_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.sessions.lock().unwrap().keys().cloned().collect();
        ids.sort();
        ids
    }
}

pub fn terminal_read_json(read: TerminalRead) -> Value {
    json!({
        "terminal_id": read.terminal_id,
        "stdout": read.stdout,
        "stderr": read.stderr,
        "running": read.running,
        "exit_code": read.exit_code,
    })
}

fn spawn_reader<R>(mut reader: R, buffer: Arc<Mutex<Vec<u8>>>)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut chunk = [0_u8; 4096];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buffer.lock().unwrap().extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
    });
}

fn shell_command(shell: Option<String>, args: Vec<String>) -> (String, Vec<String>) {
    if let Some(shell) = shell {
        return (shell, args);
    }
    #[cfg(target_os = "windows")]
    {
        (
            "cmd.exe".to_string(),
            vec!["/Q".to_string(), "/K".to_string()],
        )
    }
    #[cfg(not(target_os = "windows"))]
    {
        ("sh".to_string(), Vec::new())
    }
}

fn tail_utf8(bytes: &[u8], max_bytes: usize) -> String {
    let start = bytes.len().saturating_sub(max_bytes);
    String::from_utf8_lossy(&bytes[start..]).to_string()
}

#[allow(dead_code)]
fn default_cwd() -> &'static Path {
    Path::new(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::DbOwner;

    #[test]
    fn test_terminal_manager_create_send_read_kill() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let manager = TerminalManager::new(ProcessRegistry::new(db));
        let created = manager
            .create(TerminalCreateOptions {
                terminal_id: "term-1".to_string(),
                shell: None,
                args: Vec::new(),
                cwd: None,
            })
            .unwrap();
        assert_eq!(created.terminal_id, "term-1");
        manager.send("term-1", "echo LX_TERMINAL_READY\n").unwrap();
        let mut seen = false;
        for _ in 0..20 {
            let read = manager.read("term-1", 4096).unwrap();
            if read.stdout.contains("LX_TERMINAL_READY") {
                seen = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(seen);
        manager.kill("term-1").unwrap();
        assert!(manager.live_session_ids().is_empty());
    }
}
