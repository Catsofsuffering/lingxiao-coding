use crate::process::ProcessRegistry;
use serde_json::{json, Value};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use wait_timeout::ChildExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentToolError {
    UnsafePath,
    MissingFile,
    MissingDependency { dependency: String },
    UnsupportedType { extension: String },
    ExternalFailed { dependency: String, message: String },
    Io(String),
    Timeout { dependency: String },
}

#[derive(Clone)]
pub struct DocumentToolRunner {
    process_registry: ProcessRegistry,
}

impl DocumentToolRunner {
    pub fn new(process_registry: ProcessRegistry) -> Self {
        Self { process_registry }
    }

    pub fn parse_file(
        &self,
        request_id: &str,
        path: &Path,
        timeout_ms: u64,
    ) -> Result<Value, DocumentToolError> {
        validate_read_path(path)?;
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        match extension.as_str() {
            "txt" | "md" | "csv" | "json" | "toml" | "yaml" | "yml" | "rs" | "ts" | "js" => {
                let text = std::fs::read_to_string(path)
                    .map_err(|error| DocumentToolError::Io(error.to_string()))?;
                Ok(json!({
                    "path": path.display().to_string(),
                    "kind": "text",
                    "text": text,
                    "metadata": text_metadata(&text, "plain_text"),
                    "dependency": null,
                }))
            }
            "pdf" => self.run_stdout_tool(
                request_id,
                "pdftotext",
                vec![path.display().to_string(), "-".to_string()],
                timeout_ms,
                "pdf",
            ),
            "doc" | "docx" | "ppt" | "pptx" | "xls" | "xlsx" | "odt" | "odp" | "ods" => {
                Err(DocumentToolError::MissingDependency {
                    dependency: "soffice".to_string(),
                })
            }
            other => Err(DocumentToolError::UnsupportedType {
                extension: other.to_string(),
            }),
        }
    }

    pub fn ocr_image(
        &self,
        request_id: &str,
        path: &Path,
        timeout_ms: u64,
    ) -> Result<Value, DocumentToolError> {
        validate_read_path(path)?;
        self.run_stdout_tool(
            request_id,
            "tesseract",
            vec![path.display().to_string(), "stdout".to_string()],
            timeout_ms,
            "ocr",
        )
    }

    fn run_stdout_tool(
        &self,
        request_id: &str,
        dependency: &str,
        args: Vec<String>,
        timeout_ms: u64,
        kind: &str,
    ) -> Result<Value, DocumentToolError> {
        let child = Command::new(dependency)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        let mut child = match child {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DocumentToolError::MissingDependency {
                    dependency: dependency.to_string(),
                })
            }
            Err(error) => return Err(DocumentToolError::Io(error.to_string())),
        };

        let process_id = format!("{kind}:{request_id}");
        self.process_registry
            .register(
                process_id.clone(),
                child.id(),
                kind,
                request_id.to_string(),
                dependency.to_string(),
            )
            .map_err(|error| DocumentToolError::Io(error.to_string()))?;
        wait_child(&mut child, timeout_ms, dependency)?;
        let output = child
            .wait_with_output()
            .map_err(|error| DocumentToolError::Io(error.to_string()))?;
        let exit_code = output.status.code().unwrap_or(-1);
        if output.status.success() {
            let _ = self.process_registry.complete(&process_id, Some(exit_code));
            Ok(json!({
                "kind": kind,
                "dependency": dependency,
                "exit_code": exit_code,
                "text": String::from_utf8_lossy(&output.stdout).to_string(),
                "metadata": text_metadata(&String::from_utf8_lossy(&output.stdout), kind),
            }))
        } else {
            let message = String::from_utf8_lossy(&output.stderr)
                .lines()
                .next()
                .unwrap_or("external document tool failed")
                .to_string();
            let _ = self.process_registry.mark_failed(&process_id, &message);
            Err(DocumentToolError::ExternalFailed {
                dependency: dependency.to_string(),
                message,
            })
        }
    }
}

fn text_metadata(text: &str, layout_kind: &str) -> Value {
    let line_count = text.lines().count();
    let page_count = text.matches('\x0C').count().saturating_add(1);
    json!({
        "layout": layout_kind,
        "line_count": line_count,
        "char_count": text.chars().count(),
        "byte_count": text.len(),
        "page_count": page_count,
    })
}

pub fn document_error_payload(error: &DocumentToolError) -> Value {
    match error {
        DocumentToolError::UnsafePath => json!({"kind": "unsafe_path"}),
        DocumentToolError::MissingFile => json!({"kind": "missing_file"}),
        DocumentToolError::MissingDependency { dependency } => {
            json!({"kind": "missing_dependency", "dependency": dependency})
        }
        DocumentToolError::UnsupportedType { extension } => {
            json!({"kind": "unsupported_type", "extension": extension})
        }
        DocumentToolError::ExternalFailed { dependency, .. } => {
            json!({"kind": "external_failed", "dependency": dependency})
        }
        DocumentToolError::Io(_) => json!({"kind": "io_error"}),
        DocumentToolError::Timeout { dependency } => {
            json!({"kind": "timeout", "dependency": dependency})
        }
    }
}

pub fn document_error_message(error: &DocumentToolError) -> String {
    match error {
        DocumentToolError::UnsafePath => "Document path is unsafe".to_string(),
        DocumentToolError::MissingFile => "Document file does not exist".to_string(),
        DocumentToolError::MissingDependency { dependency } => {
            format!("Required external dependency is missing: {dependency}")
        }
        DocumentToolError::UnsupportedType { extension } => {
            format!("Unsupported document extension: {extension}")
        }
        DocumentToolError::ExternalFailed {
            dependency,
            message,
        } => {
            format!("{dependency} failed: {message}")
        }
        DocumentToolError::Io(message) => format!("Document IO failed: {message}"),
        DocumentToolError::Timeout { dependency } => {
            format!("{dependency} timed out")
        }
    }
}

fn validate_read_path(path: &Path) -> Result<(), DocumentToolError> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(DocumentToolError::UnsafePath);
    }
    if !path.exists() || !path.is_file() {
        return Err(DocumentToolError::MissingFile);
    }
    Ok(())
}

fn wait_child(
    child: &mut Child,
    timeout_ms: u64,
    dependency: &str,
) -> Result<(), DocumentToolError> {
    match child
        .wait_timeout(Duration::from_millis(timeout_ms))
        .map_err(|error| DocumentToolError::Io(error.to_string()))?
    {
        Some(_) => Ok(()),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            Err(DocumentToolError::Timeout {
                dependency: dependency.to_string(),
            })
        }
    }
}

#[allow(dead_code)]
fn pathbuf(path: impl Into<PathBuf>) -> PathBuf {
    path.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::DbOwner;

    #[test]
    fn test_parse_text_file_without_external_dependency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        std::fs::write(&path, "hello docs").unwrap();
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let runner = DocumentToolRunner::new(ProcessRegistry::new(db));
        let result = runner.parse_file("doc-1", &path, 1_000).unwrap();
        assert_eq!(result["kind"], "text");
        assert_eq!(result["text"], "hello docs");
        assert_eq!(result["metadata"]["layout"], "plain_text");
        assert_eq!(result["metadata"]["line_count"], 1);
        assert_eq!(result["metadata"]["byte_count"], 10);
    }

    #[test]
    fn test_office_file_reports_typed_missing_dependency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("doc.docx");
        std::fs::write(&path, "placeholder").unwrap();
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let runner = DocumentToolRunner::new(ProcessRegistry::new(db));
        let error = runner.parse_file("doc-2", &path, 1_000).unwrap_err();
        assert_eq!(
            document_error_payload(&error),
            json!({"kind": "missing_dependency", "dependency": "soffice"})
        );
    }
}
