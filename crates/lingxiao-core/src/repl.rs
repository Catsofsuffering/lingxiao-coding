use crate::process::{configure_command_for_process_tree, kill_child_tree, ProcessRegistry};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use wait_timeout::ChildExt;

#[derive(Debug, Clone)]
pub struct ReplEvalRequest {
    pub eval_id: String,
    pub language: String,
    pub code: String,
    pub cwd: Option<PathBuf>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplEvalError {
    MissingRuntime { language: String },
    SpawnFailed(String),
    Timeout,
    WaitFailed(String),
}

#[derive(Clone)]
pub struct ReplRunner {
    process_registry: ProcessRegistry,
}

impl ReplRunner {
    pub fn new(process_registry: ProcessRegistry) -> Self {
        Self { process_registry }
    }

    pub fn eval(&self, request: ReplEvalRequest) -> Result<Value, ReplEvalError> {
        let candidates = runtime_candidates(&request.language, &request.code)?;
        let mut last_spawn_error = None;
        for (program, args) in candidates {
            match spawn_eval(&program, &args, request.cwd.as_ref()) {
                Ok(child) => return self.wait_eval(request, program, child),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    last_spawn_error = Some(error.to_string());
                }
                Err(error) => return Err(ReplEvalError::SpawnFailed(error.to_string())),
            }
        }
        let _ = last_spawn_error;
        Err(ReplEvalError::MissingRuntime {
            language: request.language,
        })
    }

    fn wait_eval(
        &self,
        request: ReplEvalRequest,
        program: String,
        mut child: Child,
    ) -> Result<Value, ReplEvalError> {
        let pid = child.id();
        self.process_registry
            .register(
                format!("repl:{}", request.eval_id),
                pid,
                "repl",
                request.eval_id.clone(),
                program.clone(),
            )
            .map_err(|error| ReplEvalError::SpawnFailed(error.to_string()))?;

        let wait_status = child
            .wait_timeout(Duration::from_millis(request.timeout_ms))
            .map_err(|error| {
                let _ = self
                    .process_registry
                    .mark_failed(&format!("repl:{}", request.eval_id), &error.to_string());
                ReplEvalError::WaitFailed(error.to_string())
            })?;
        match wait_status {
            Some(_) => {}
            None => {
                let _ = kill_child_tree(&mut child);
                let _ = child.wait();
                let _ = self
                    .process_registry
                    .mark_failed(&format!("repl:{}", request.eval_id), "timeout");
                return Err(ReplEvalError::Timeout);
            }
        }

        let output = child
            .wait_with_output()
            .map_err(|error| ReplEvalError::WaitFailed(error.to_string()))?;
        let exit_code = output.status.code().unwrap_or(-1);
        let _ = self
            .process_registry
            .complete(&format!("repl:{}", request.eval_id), Some(exit_code));
        Ok(json!({
            "eval_id": request.eval_id,
            "language": request.language,
            "runtime": program,
            "pid": pid,
            "exit_code": exit_code,
            "success": output.status.success(),
            "stdout": String::from_utf8_lossy(&output.stdout).to_string(),
            "stderr": String::from_utf8_lossy(&output.stderr).to_string(),
        }))
    }
}

fn runtime_candidates(
    language: &str,
    code: &str,
) -> Result<Vec<(String, Vec<String>)>, ReplEvalError> {
    match language.to_ascii_lowercase().as_str() {
        "node" | "javascript" | "js" => Ok(vec![(
            "node".to_string(),
            vec!["-e".to_string(), code.to_string()],
        )]),
        "python" | "py" => Ok(vec![
            (
                "python".to_string(),
                vec!["-c".to_string(), code.to_string()],
            ),
            (
                "py".to_string(),
                vec!["-3".to_string(), "-c".to_string(), code.to_string()],
            ),
        ]),
        other => Err(ReplEvalError::MissingRuntime {
            language: other.to_string(),
        }),
    }
}

fn spawn_eval(program: &str, args: &[String], cwd: Option<&PathBuf>) -> std::io::Result<Child> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    configure_command_for_process_tree(&mut command);
    command.spawn()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::DbOwner;

    #[test]
    fn test_repl_runner_missing_runtime_is_typed() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let runner = ReplRunner::new(ProcessRegistry::new(db));
        let err = runner
            .eval(ReplEvalRequest {
                eval_id: "missing".to_string(),
                language: "definitely-not-a-runtime".to_string(),
                code: "1".to_string(),
                cwd: None,
                timeout_ms: 100,
            })
            .unwrap_err();
        assert_eq!(
            err,
            ReplEvalError::MissingRuntime {
                language: "definitely-not-a-runtime".to_string()
            }
        );
    }
}
