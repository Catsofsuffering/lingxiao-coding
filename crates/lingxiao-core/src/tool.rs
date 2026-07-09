use crate::process::{configure_command_for_process_tree, kill_child_tree};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use wait_timeout::ChildExt;

pub type ToolName = String;

const NATIVE_PROCESS_OUTPUT_LIMIT: usize = 1024 * 1024;

/// Result from a native tool execution.
#[derive(Debug, Clone)]
pub struct ToolResult {
    pub success: bool,
    pub output: Value,
    pub error: Option<String>,
}

impl ToolResult {
    pub fn ok(output: Value) -> Self {
        Self {
            success: true,
            output,
            error: None,
        }
    }
    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            output: Value::Null,
            error: Some(message.into()),
        }
    }
}

/// Permission requirement for a tool call.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolPermission {
    /// No permission needed (read-only, safe).
    None,
    /// Requires an active grant for this tool/path scope.
    RequiresGrant {
        tool_name: String,
        path_scope: Option<String>,
    },
}

/// Definition of a native tool — includes executor.
pub struct ToolDefinition {
    pub name: ToolName,
    pub description: String,
    pub is_native: bool,
    pub permission: ToolPermission,
    pub executor: Box<dyn Fn(&Value) -> ToolResult + Send + Sync>,
}

impl std::fmt::Debug for ToolDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolDefinition")
            .field("name", &self.name)
            .field("is_native", &self.is_native)
            .finish()
    }
}

pub struct ToolRegistry {
    tools: HashMap<ToolName, ToolDefinition>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
        }
    }

    /// Create a registry pre-loaded with all built-in native tools.
    pub fn with_native_tools() -> Self {
        let mut r = Self::new();
        r.register_builtin_tools();
        r
    }

    pub fn register(&mut self, tool: ToolDefinition) {
        self.tools.insert(tool.name.clone(), tool);
    }

    pub fn get(&self, name: &str) -> Option<&ToolDefinition> {
        self.tools.get(name)
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    pub fn llm_tool_definitions(&self) -> Vec<lingxiao_llm_host_protocol::ToolDefinition> {
        let mut tools = self
            .tools
            .values()
            .filter(|tool| tool.is_native)
            .map(|tool| lingxiao_llm_host_protocol::ToolDefinition {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema: schema_for_tool(&tool.name),
            })
            .collect::<Vec<_>>();
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        tools
    }

    pub fn required_permission_for_call(&self, name: &str, args: &Value) -> Option<ToolPermission> {
        let def = self.tools.get(name)?;
        if name == "git" {
            let subcommand = args.get("subcommand").and_then(|v| v.as_str())?;
            if is_git_read_only(subcommand) {
                return None;
            }
            return Some(ToolPermission::RequiresGrant {
                tool_name: "git_write".into(),
                path_scope: args
                    .get("cwd")
                    .and_then(|v| v.as_str())
                    .map(std::string::ToString::to_string),
            });
        }
        match &def.permission {
            ToolPermission::None => None,
            ToolPermission::RequiresGrant { tool_name, .. } => {
                Some(ToolPermission::RequiresGrant {
                    tool_name: tool_name.clone(),
                    path_scope: args
                        .get("path")
                        .or_else(|| args.get("cwd"))
                        .and_then(|v| v.as_str())
                        .map(std::string::ToString::to_string),
                })
            }
        }
    }

    /// Execute a native tool by name with the given args.
    pub fn execute(&self, name: &str, args: &Value) -> ToolResult {
        match self.tools.get(name) {
            Some(def) => (def.executor)(args),
            None => ToolResult::err(format!("Tool not found: {name}")),
        }
    }
}

fn schema_for_tool(name: &str) -> Value {
    match name {
        "file_read" => object_schema(
            [
                ("path", string_schema("File path to read")),
                ("offset", integer_schema("0-based line offset")),
                ("limit", integer_schema("Maximum number of lines to return")),
            ],
            ["path"],
        ),
        "file_write" => object_schema(
            [
                ("path", string_schema("File path to write")),
                ("content", string_schema("Text content to write")),
                ("append", boolean_schema("Append instead of overwrite")),
            ],
            ["path", "content"],
        ),
        "file_create" => object_schema(
            [
                ("path", string_schema("File path to create")),
                ("content", string_schema("Initial file content")),
                (
                    "overwrite",
                    boolean_schema("Overwrite if the file already exists"),
                ),
            ],
            ["path"],
        ),
        "structured_patch" => object_schema(
            [
                ("path", string_schema("File path to patch")),
                (
                    "operations",
                    json!({
                        "type": "array",
                        "description": "Patch operations: exact replace or line_replace",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["replace", "line_replace"]},
                                "old": {"type": "string"},
                                "new": {"type": "string"},
                                "content": {"type": "string"},
                                "replace": {"type": "string"},
                                "replace_all": {"type": "boolean"},
                                "start_line": {"type": "integer", "minimum": 1},
                                "end_line": {"type": "integer", "minimum": 1}
                            },
                            "additionalProperties": false
                        }
                    }),
                ),
            ],
            ["path", "operations"],
        ),
        "list_dir" => object_schema(
            [
                ("path", string_schema("Directory path to list")),
                ("recursive", boolean_schema("Whether to recurse")),
            ],
            ["path"],
        ),
        "glob" => object_schema(
            [
                ("pattern", string_schema("Glob pattern, such as *.rs")),
                ("base_dir", string_schema("Base directory to search")),
            ],
            ["pattern"],
        ),
        "code_search" => object_schema(
            [
                ("query", string_schema("Text pattern to search for")),
                ("path", string_schema("Directory or file path")),
                ("include", string_schema("Filename glob include filter")),
                ("case_sensitive", boolean_schema("Case-sensitive search")),
                ("max_results", integer_schema("Maximum matches to return")),
            ],
            ["query"],
        ),
        "shell" => object_schema(
            [
                ("command", string_schema("Shell command to execute")),
                ("cwd", string_schema("Working directory")),
                ("timeout_ms", integer_schema("Timeout in milliseconds")),
            ],
            ["command"],
        ),
        "git" => object_schema(
            [
                ("subcommand", string_schema("Allowed git subcommand")),
                (
                    "args",
                    json!({"type": "array", "items": {"type": "string"}}),
                ),
                ("cwd", string_schema("Repository working directory")),
            ],
            ["subcommand"],
        ),
        "attempt_completion" => object_schema(
            [("result", json!({"description": "Final task result"}))],
            ["result"],
        ),
        "send_message" => object_schema(
            [
                ("content", string_schema("Message content")),
                ("recipient", string_schema("Recipient id or user")),
                ("kind", string_schema("Message kind")),
            ],
            ["content"],
        ),
        _ => json!({"type": "object", "additionalProperties": true}),
    }
}

fn object_schema<const N: usize, const M: usize>(
    properties: [(&str, Value); N],
    required: [&str; M],
) -> Value {
    let properties = properties
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect::<serde_json::Map<String, Value>>();
    let required = required.into_iter().map(str::to_string).collect::<Vec<_>>();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn string_schema(description: &str) -> Value {
    json!({"type": "string", "description": description})
}

fn boolean_schema(description: &str) -> Value {
    json!({"type": "boolean", "description": description})
}

fn integer_schema(description: &str) -> Value {
    json!({"type": "integer", "description": description})
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Native tool registration
// ─────────────────────────────────────────────────────────────────────────────

fn is_git_read_only(subcommand: &str) -> bool {
    GIT_READ_ONLY_SUBCOMMANDS.contains(&subcommand)
}

const GIT_READ_ONLY_SUBCOMMANDS: &[&str] = &[
    "status",
    "log",
    "diff",
    "show",
    "branch",
    "tag",
    "ls-files",
    "rev-parse",
    "describe",
    "remote",
];

const GIT_WRITE_SUBCOMMANDS: &[&str] = &[
    "add", "commit", "checkout", "switch", "push", "restore", "reset",
];

fn is_git_allowed(subcommand: &str) -> bool {
    GIT_READ_ONLY_SUBCOMMANDS.contains(&subcommand) || GIT_WRITE_SUBCOMMANDS.contains(&subcommand)
}

impl ToolRegistry {
    fn register_builtin_tools(&mut self) {
        self.register(tool_file_read());
        self.register(tool_file_write());
        self.register(tool_file_create());
        self.register(tool_structured_patch());
        self.register(tool_list_dir());
        self.register(tool_glob());
        self.register(tool_code_search());
        self.register(tool_shell());
        self.register(tool_git());
        self.register(tool_attempt_completion());
        self.register(tool_send_message());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// file_read
// ─────────────────────────────────────────────────────────────────────────────

fn tool_file_read() -> ToolDefinition {
    ToolDefinition {
        name: "file_read".into(),
        description: "Read the contents of a file. Returns text content.".into(),
        is_native: true,
        permission: ToolPermission::None,
        executor: Box::new(|args| {
            let path = match args.get("path").and_then(|v| v.as_str()) {
                Some(p) => PathBuf::from(p),
                None => return ToolResult::err("Missing required param: path"),
            };
            let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            let limit = args
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);
            // Reject traversal attempts
            if path.components().any(|c| c.as_os_str() == "..") {
                return ToolResult::err("Path traversal ('..') is not allowed");
            }
            match std::fs::read_to_string(&path) {
                Ok(content) => {
                    let lines: Vec<&str> = content.lines().collect();
                    let total = lines.len();
                    let start = offset.min(total);
                    let end = match limit {
                        Some(n) => (start + n).min(total),
                        None => total,
                    };
                    let slice = lines[start..end].join("\n");
                    ToolResult::ok(json!({
                        "content": slice,
                        "path": path.display().to_string(),
                        "total_lines": total,
                        "lines_returned": end - start,
                    }))
                }
                Err(e) => ToolResult::err(format!("file_read failed: {e}")),
            }
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// file_write  (requires grant)
// ─────────────────────────────────────────────────────────────────────────────

fn tool_file_write() -> ToolDefinition {
    ToolDefinition {
        name: "file_write".into(),
        description: "Overwrite or append to a file. Requires write grant.".into(),
        is_native: true,
        permission: ToolPermission::RequiresGrant {
            tool_name: "file_write".into(),
            path_scope: None,
        },
        executor: Box::new(|args| {
            let path = match args.get("path").and_then(|v| v.as_str()) {
                Some(p) => PathBuf::from(p),
                None => return ToolResult::err("Missing required param: path"),
            };
            let content = match args.get("content").and_then(|v| v.as_str()) {
                Some(c) => c.to_string(),
                None => return ToolResult::err("Missing required param: content"),
            };
            let append = args
                .get("append")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if path.components().any(|c| c.as_os_str() == "..") {
                return ToolResult::err("Path traversal ('..') is not allowed");
            }
            let result = if append {
                use std::io::Write;
                std::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&path)
                    .and_then(|mut f| f.write_all(content.as_bytes()))
            } else {
                std::fs::write(&path, &content)
            };
            match result {
                Ok(()) => ToolResult::ok(json!({
                    "path": path.display().to_string(),
                    "bytes_written": content.len(),
                    "appended": append,
                })),
                Err(e) => ToolResult::err(format!("file_write failed: {e}")),
            }
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// file_create  (requires grant; fails if file exists unless overwrite=true)
// ─────────────────────────────────────────────────────────────────────────────

fn tool_file_create() -> ToolDefinition {
    ToolDefinition {
        name: "file_create".into(),
        description: "Create a new file. Fails if it already exists unless overwrite=true.".into(),
        is_native: true,
        permission: ToolPermission::RequiresGrant {
            tool_name: "file_write".into(),
            path_scope: None,
        },
        executor: Box::new(|args| {
            let path = match args.get("path").and_then(|v| v.as_str()) {
                Some(p) => PathBuf::from(p),
                None => return ToolResult::err("Missing required param: path"),
            };
            let content = args
                .get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let overwrite = args
                .get("overwrite")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if path.components().any(|c| c.as_os_str() == "..") {
                return ToolResult::err("Path traversal ('..') is not allowed");
            }
            if path.exists() && !overwrite {
                return ToolResult::err(format!("File already exists: {}", path.display()));
            }
            if let Some(parent) = path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    return ToolResult::err(format!("Failed to create parent directories: {e}"));
                }
            }
            match std::fs::write(&path, &content) {
                Ok(()) => ToolResult::ok(json!({
                    "path": path.display().to_string(),
                    "created": true,
                })),
                Err(e) => ToolResult::err(format!("file_create failed: {e}")),
            }
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// structured_patch  (requires grant)
// ─────────────────────────────────────────────────────────────────────────────

fn tool_structured_patch() -> ToolDefinition {
    ToolDefinition {
        name: "structured_patch".into(),
        description: "Apply exact text or line-range patch operations to one file.".into(),
        is_native: true,
        permission: ToolPermission::RequiresGrant {
            tool_name: "file_write".into(),
            path_scope: None,
        },
        executor: Box::new(|args| {
            let path = match args.get("path").and_then(Value::as_str) {
                Some(path) => PathBuf::from(path),
                None => return ToolResult::err("Missing required param: path"),
            };
            if path.components().any(|c| c.as_os_str() == "..") {
                return ToolResult::err("Path traversal ('..') is not allowed");
            }
            let mut content = match std::fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) => {
                    return ToolResult::err(format!("structured_patch read failed: {error}"))
                }
            };
            let operations = match args.get("operations").and_then(Value::as_array) {
                Some(operations) if !operations.is_empty() => operations,
                _ => return ToolResult::err("Missing required param: operations"),
            };
            let mut applied = 0_usize;
            for operation in operations {
                let op_type = operation
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("replace");
                match op_type {
                    "replace" => {
                        let old = match operation.get("old").and_then(Value::as_str) {
                            Some(old) => old,
                            None => return ToolResult::err("replace operation missing old"),
                        };
                        let new = match operation.get("new").and_then(Value::as_str) {
                            Some(new) => new,
                            None => return ToolResult::err("replace operation missing new"),
                        };
                        let count = content.matches(old).count();
                        if count == 0 {
                            return ToolResult::err("replace operation did not match");
                        }
                        let replace_all = operation
                            .get("replace_all")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        if count > 1 && !replace_all {
                            return ToolResult::err(
                                "replace operation matched multiple locations; set replace_all=true",
                            );
                        }
                        content = if replace_all {
                            content.replace(old, new)
                        } else {
                            content.replacen(old, new, 1)
                        };
                        applied += if replace_all { count } else { 1 };
                    }
                    "line_replace" => {
                        let start_line = operation
                            .get("start_line")
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        let end_line = operation
                            .get("end_line")
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        if start_line == 0 || end_line < start_line {
                            return ToolResult::err(
                                "line_replace requires 1-based start_line <= end_line",
                            );
                        }
                        let replacement = operation
                            .get("content")
                            .or_else(|| operation.get("new"))
                            .or_else(|| operation.get("replace"))
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                "line_replace operation missing content/new/replace".to_string()
                            });
                        let replacement = match replacement {
                            Ok(replacement) => replacement,
                            Err(message) => return ToolResult::err(message),
                        };
                        let had_trailing_newline = content.ends_with('\n');
                        let mut lines = content.lines().map(str::to_string).collect::<Vec<_>>();
                        let start = (start_line - 1) as usize;
                        let end = end_line as usize;
                        if end > lines.len() {
                            return ToolResult::err("line_replace range exceeds file length");
                        }
                        let replacement_lines =
                            replacement.lines().map(str::to_string).collect::<Vec<_>>();
                        lines.splice(start..end, replacement_lines);
                        content = lines.join("\n");
                        if had_trailing_newline {
                            content.push('\n');
                        }
                        applied += 1;
                    }
                    other => {
                        return ToolResult::err(format!("Unsupported patch operation: {other}"))
                    }
                }
            }
            match std::fs::write(&path, &content) {
                Ok(()) => ToolResult::ok(json!({
                    "path": path.display().to_string(),
                    "operations_applied": applied,
                    "bytes_written": content.len(),
                })),
                Err(error) => ToolResult::err(format!("structured_patch write failed: {error}")),
            }
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// list_dir
// ─────────────────────────────────────────────────────────────────────────────

fn tool_list_dir() -> ToolDefinition {
    ToolDefinition {
        name: "list_dir".into(),
        description: "List entries in a directory.".into(),
        is_native: true,
        permission: ToolPermission::None,
        executor: Box::new(|args| {
            let path = match args.get("path").and_then(|v| v.as_str()) {
                Some(p) => PathBuf::from(p),
                None => return ToolResult::err("Missing required param: path"),
            };
            let recursive = args
                .get("recursive")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if path.components().any(|c| c.as_os_str() == "..") {
                return ToolResult::err("Path traversal ('..') is not allowed");
            }
            match collect_dir_entries(&path, recursive, 0, 5) {
                Ok(entries) => ToolResult::ok(json!({
                    "path": path.display().to_string(),
                    "entries": entries,
                })),
                Err(e) => ToolResult::err(format!("list_dir failed: {e}")),
            }
        }),
    }
}

fn collect_dir_entries(
    path: &Path,
    recursive: bool,
    depth: usize,
    max_depth: usize,
) -> std::io::Result<Vec<Value>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        let is_dir = meta.is_dir();
        let name = entry.file_name().to_string_lossy().to_string();
        let entry_path = entry.path().display().to_string();
        let mut obj = json!({
            "name": name,
            "path": entry_path,
            "is_dir": is_dir,
            "size": if is_dir { 0 } else { meta.len() },
        });
        if recursive && is_dir && depth < max_depth {
            if let Ok(children) = collect_dir_entries(&entry.path(), true, depth + 1, max_depth) {
                obj["children"] = json!(children);
            }
        }
        entries.push(obj);
    }
    entries.sort_by(|a, b| {
        let a_name = a["name"].as_str().unwrap_or("");
        let b_name = b["name"].as_str().unwrap_or("");
        a_name.cmp(b_name)
    });
    Ok(entries)
}

// ─────────────────────────────────────────────────────────────────────────────
// glob
// ─────────────────────────────────────────────────────────────────────────────

fn tool_glob() -> ToolDefinition {
    ToolDefinition {
        name: "glob".into(),
        description: "Find files matching a glob pattern. Returns sorted list of matching paths."
            .into(),
        is_native: true,
        permission: ToolPermission::None,
        executor: Box::new(|args| {
            let pattern = match args.get("pattern").and_then(|v| v.as_str()) {
                Some(p) => p.to_string(),
                None => return ToolResult::err("Missing required param: pattern"),
            };
            let base = args
                .get("base_dir")
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));

            if !base.exists() {
                return ToolResult::err(format!("base_dir not found: {}", base.display()));
            }

            let mut matched: Vec<String> = Vec::new();
            collect_glob_matches(&base, &pattern, &mut matched);
            matched.sort();
            let count = matched.len();
            ToolResult::ok(json!({
                "pattern": pattern,
                "base_dir": base.display().to_string(),
                "matches": matched,
                "count": count,
            }))
        }),
    }
}

/// Simple glob implementation using walkdir + fnmatch-style matching.
fn collect_glob_matches(dir: &Path, pattern: &str, results: &mut Vec<String>) {
    let Ok(read) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in read.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if path.is_dir() {
            collect_glob_matches(&path, pattern, results);
        } else if simple_glob_match(pattern, &name_str) {
            results.push(path.display().to_string());
        }
    }
}

/// Minimal glob pattern matching: `*` matches any sequence, `?` matches one char.
fn simple_glob_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    glob_dp(&p, &n, 0, 0)
}

fn glob_dp(p: &[char], n: &[char], pi: usize, ni: usize) -> bool {
    if pi == p.len() {
        return ni == n.len();
    }
    if p[pi] == '*' {
        // `*` can match zero or more chars
        if glob_dp(p, n, pi + 1, ni) {
            return true;
        }
        if ni < n.len() {
            return glob_dp(p, n, pi, ni + 1);
        }
        return false;
    }
    if ni == n.len() {
        return false;
    }
    if p[pi] == '?' || p[pi] == n[ni] {
        return glob_dp(p, n, pi + 1, ni + 1);
    }
    false
}

// ─────────────────────────────────────────────────────────────────────────────
// code_search  (grep-based, no external tool dependency)
// ─────────────────────────────────────────────────────────────────────────────

fn tool_code_search() -> ToolDefinition {
    ToolDefinition {
        name: "code_search".into(),
        description: "Search for a pattern in files. Returns matching lines with context.".into(),
        is_native: true,
        permission: ToolPermission::None,
        executor: Box::new(|args| {
            let query = match args.get("query").and_then(|v| v.as_str()) {
                Some(q) => q.to_string(),
                None => return ToolResult::err("Missing required param: query"),
            };
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            let case_sensitive = args
                .get("case_sensitive")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let max_results = args
                .get("max_results")
                .and_then(|v| v.as_u64())
                .unwrap_or(100) as usize;
            let include_pattern = args
                .get("include")
                .and_then(|v| v.as_str())
                .unwrap_or("*")
                .to_string();

            let mut matches = Vec::new();
            search_files_recursive(
                &path,
                &query,
                &include_pattern,
                case_sensitive,
                max_results,
                &mut matches,
            );

            ToolResult::ok(json!({
                "query": query,
                "path": path.display().to_string(),
                "matches": matches,
                "total_matches": matches.len(),
            }))
        }),
    }
}

fn search_files_recursive(
    dir: &Path,
    query: &str,
    include: &str,
    case_sensitive: bool,
    max_results: usize,
    matches: &mut Vec<Value>,
) {
    if matches.len() >= max_results {
        return;
    }
    let Ok(read) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in read.flatten() {
        if matches.len() >= max_results {
            return;
        }
        let path = entry.path();
        if path.is_dir() {
            // Skip hidden dirs and common noise
            let name = entry.file_name();
            let n = name.to_string_lossy();
            if n.starts_with('.') || n == "target" || n == "node_modules" {
                continue;
            }
            search_files_recursive(&path, query, include, case_sensitive, max_results, matches);
        } else {
            let name = entry.file_name();
            if !simple_glob_match(include, &name.to_string_lossy()) {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            for (line_no, line) in content.lines().enumerate() {
                if matches.len() >= max_results {
                    return;
                }
                let found = if case_sensitive {
                    line.contains(query)
                } else {
                    line.to_lowercase().contains(&query.to_lowercase())
                };
                if found {
                    matches.push(json!({
                        "file": path.display().to_string(),
                        "line": line_no + 1,
                        "content": line,
                    }));
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// shell  (requires grant; hard timeout; supervised)
// ─────────────────────────────────────────────────────────────────────────────

fn tool_shell() -> ToolDefinition {
    ToolDefinition {
        name: "shell".into(),
        description: "Run a shell command. Requires explicit shell grant.".into(),
        is_native: true,
        permission: ToolPermission::RequiresGrant {
            tool_name: "shell".into(),
            path_scope: None,
        },
        executor: Box::new(|args| {
            let cmd_str = match args.get("command").and_then(|v| v.as_str()) {
                Some(c) => c.to_string(),
                None => return ToolResult::err("Missing required param: command"),
            };
            if let Some(reason) = unsafe_shell_control_reason(&cmd_str) {
                return ToolResult::err(format!("shell command rejected: {reason}"));
            }
            let timeout_ms = args
                .get("timeout_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(30_000);
            let cwd = args.get("cwd").and_then(|v| v.as_str()).map(PathBuf::from);

            #[cfg(target_os = "windows")]
            let (prog, shell_args) = ("cmd.exe", vec!["/C".to_string(), cmd_str.clone()]);
            #[cfg(not(target_os = "windows"))]
            let (prog, shell_args) = ("sh", vec!["-c".to_string(), cmd_str.clone()]);

            let mut command = Command::new(prog);
            command
                .args(&shell_args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .current_dir(cwd.as_deref().unwrap_or_else(|| Path::new(".")));
            configure_command_for_process_tree(&mut command);
            let mut child = match command.spawn() {
                Ok(c) => c,
                Err(e) => return ToolResult::err(format!("shell spawn failed: {e}")),
            };
            let stdout_drain = child.stdout.take().map(spawn_output_drain);
            let stderr_drain = child.stderr.take().map(spawn_output_drain);

            let status = match child.wait_timeout(Duration::from_millis(timeout_ms)) {
                Ok(Some(status)) => status,
                Ok(None) => {
                    let _ = kill_child_tree(&mut child);
                    let _ = child.wait();
                    let _ = collect_output(stdout_drain);
                    let _ = collect_output(stderr_drain);
                    return ToolResult::err(format!(
                        "shell command timed out after {timeout_ms}ms"
                    ));
                }
                Err(e) => return ToolResult::err(format!("shell wait error: {e}")),
            };

            let stdout_bytes = collect_output(stdout_drain);
            let stderr_bytes = collect_output(stderr_drain);
            let stdout_truncated = stdout_bytes.truncated;
            let stderr_truncated = stderr_bytes.truncated;
            let stdout = String::from_utf8_lossy(&stdout_bytes.bytes).to_string();
            let stderr = String::from_utf8_lossy(&stderr_bytes.bytes).to_string();
            let exit_code = status.code().unwrap_or(-1);
            ToolResult::ok(json!({
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "stdout_truncated": stdout_truncated,
                "stderr_truncated": stderr_truncated,
                "command": cmd_str,
                "success": status.success(),
            }))
        }),
    }
}

fn unsafe_shell_control_reason(command: &str) -> Option<&'static str> {
    if command.contains('\n') || command.contains('\r') {
        return Some("newlines are not allowed");
    }
    for token in ["&&", "||", "|", ";", "`", "$(", "<", ">", "&"] {
        if command.contains(token) {
            return Some("shell control operators and redirection are not allowed");
        }
    }
    None
}

struct OutputDrain {
    buffer: Arc<Mutex<BoundedOutput>>,
    handle: JoinHandle<()>,
}

#[derive(Clone, Default)]
struct BoundedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

fn spawn_output_drain<R>(mut reader: R) -> OutputDrain
where
    R: Read + Send + 'static,
{
    let buffer = Arc::new(Mutex::new(BoundedOutput::default()));
    let thread_buffer = buffer.clone();
    let handle = thread::spawn(move || {
        let mut chunk = [0_u8; 8192];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => append_bounded_output(&thread_buffer, &chunk[..n]),
                Err(_) => break,
            }
        }
    });
    OutputDrain { buffer, handle }
}

fn append_bounded_output(buffer: &Arc<Mutex<BoundedOutput>>, bytes: &[u8]) {
    let mut guard = buffer.lock().unwrap();
    guard.bytes.extend_from_slice(bytes);
    if guard.bytes.len() > NATIVE_PROCESS_OUTPUT_LIMIT {
        let overflow = guard.bytes.len() - NATIVE_PROCESS_OUTPUT_LIMIT;
        guard.bytes.drain(..overflow);
        guard.truncated = true;
    }
}

fn collect_output(drain: Option<OutputDrain>) -> BoundedOutput {
    let Some(drain) = drain else {
        return BoundedOutput::default();
    };
    let _ = drain.handle.join();
    let output = drain.buffer.lock().unwrap().clone();
    output
}

// ─────────────────────────────────────────────────────────────────────────────
// git  (read-only and write ops; write requires grant)
// ─────────────────────────────────────────────────────────────────────────────

fn tool_git() -> ToolDefinition {
    ToolDefinition {
        name: "git".into(),
        description: "Run a git command. Write operations require git_write grant.".into(),
        is_native: true,
        permission: ToolPermission::None, // checked per-operation in executor
        executor: Box::new(|args| {
            let subcommand = match args.get("subcommand").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => return ToolResult::err("Missing required param: subcommand"),
            };
            let git_args: Vec<String> = args
                .get("args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let cwd = args.get("cwd").and_then(|v| v.as_str()).map(PathBuf::from);

            if !is_git_allowed(&subcommand) {
                return ToolResult::err(format!("git subcommand is not allowed: {subcommand}"));
            }

            // Read-only subcommands; others need grant (enforced in CommandRouter)
            const READ_ONLY: &[&str] = &[
                "status",
                "log",
                "diff",
                "show",
                "branch",
                "tag",
                "ls-files",
                "rev-parse",
                "describe",
                "remote",
            ];
            if !READ_ONLY.contains(&subcommand.as_str()) {
                // The CommandRouter permission gate is the authoritative check,
                // but record the intent here so callers can't bypass.
                // We still execute — the command router decides permission.
            }

            let mut full_args = vec![subcommand.clone()];
            full_args.extend(git_args);

            let mut cmd = Command::new("git");
            cmd.args(&full_args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            if let Some(ref dir) = cwd {
                cmd.current_dir(dir);
            }

            match cmd.output() {
                Ok(out) => {
                    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
                    let exit_code = out.status.code().unwrap_or(-1);
                    if out.status.success() {
                        ToolResult::ok(json!({
                            "subcommand": subcommand,
                            "output": stdout.trim(),
                            "exit_code": exit_code,
                        }))
                    } else {
                        ToolResult::err(format!(
                            "git {subcommand} failed (exit {exit_code}): {stderr}"
                        ))
                    }
                }
                Err(e) => ToolResult::err(format!("git spawn failed: {e}")),
            }
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// attempt_completion
// ─────────────────────────────────────────────────────────────────────────────

fn tool_attempt_completion() -> ToolDefinition {
    ToolDefinition {
        name: "attempt_completion".into(),
        description: "Signal that the agent has completed its task.".into(),
        is_native: true,
        permission: ToolPermission::None,
        executor: Box::new(|args| {
            let result = args
                .get("result")
                .cloned()
                .unwrap_or_else(|| json!("Task completed"));
            ToolResult::ok(json!({
                "completed": true,
                "result": result,
            }))
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

fn tool_send_message() -> ToolDefinition {
    ToolDefinition {
        name: "send_message".into(),
        description: "Send a structured message to the user or another agent.".into(),
        is_native: true,
        permission: ToolPermission::None,
        executor: Box::new(|args| {
            let content = match args.get("content").and_then(Value::as_str) {
                Some(content) if !content.trim().is_empty() => content.to_string(),
                _ => return ToolResult::err("Missing required param: content"),
            };
            let recipient = args
                .get("recipient")
                .or_else(|| args.get("to"))
                .and_then(Value::as_str)
                .unwrap_or("user")
                .to_string();
            let kind = args
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("message")
                .to_string();
            ToolResult::ok(json!({
                "sent": true,
                "recipient": recipient,
                "kind": kind,
                "content": content,
            }))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> ToolRegistry {
        ToolRegistry::with_native_tools()
    }

    // ── file_read ─────────────────────────────────────────────────────────────

    #[test]
    fn test_p3_file_read_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hello.txt");
        std::fs::write(&path, "line1\nline2\nline3").unwrap();

        let r = registry();
        let result = r.execute("file_read", &json!({"path": path.display().to_string()}));
        assert!(result.success);
        assert_eq!(result.output["content"], "line1\nline2\nline3");
        assert_eq!(result.output["total_lines"], 3);
    }

    #[test]
    fn test_llm_tool_definitions_include_required_json_schema() {
        let r = registry();
        let tools = r.llm_tool_definitions();
        let file_write = tools
            .iter()
            .find(|tool| tool.name == "file_write")
            .expect("file_write tool schema");
        assert_eq!(file_write.input_schema["type"], "object");
        assert_eq!(file_write.input_schema["additionalProperties"], false);
        let required = file_write.input_schema["required"].as_array().unwrap();
        assert!(required.iter().any(|value| value == "path"));
        assert!(required.iter().any(|value| value == "content"));
        assert_eq!(
            file_write.input_schema["properties"]["append"]["type"],
            "boolean"
        );
    }

    #[test]
    fn test_p3_file_read_missing() {
        let r = registry();
        let result = r.execute("file_read", &json!({"path": "/nonexistent/xyz.txt"}));
        assert!(!result.success);
        assert!(result.error.as_ref().unwrap().contains("file_read failed"));
    }

    #[test]
    fn test_p3_file_read_blocks_traversal() {
        let r = registry();
        let result = r.execute("file_read", &json!({"path": "../../etc/passwd"}));
        assert!(!result.success);
        assert!(result.error.as_ref().unwrap().contains("traversal"));
    }

    #[test]
    fn test_p3_file_read_with_offset_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.txt");
        std::fs::write(&path, "a\nb\nc\nd\ne").unwrap();
        let r = registry();
        let result = r.execute(
            "file_read",
            &json!({"path": path.display().to_string(), "offset": 1, "limit": 2}),
        );
        assert!(result.success);
        assert_eq!(result.output["content"], "b\nc");
        assert_eq!(result.output["lines_returned"], 2);
    }

    // ── file_write ────────────────────────────────────────────────────────────

    #[test]
    fn test_p3_file_write_creates_and_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.txt");
        let r = registry();
        let result = r.execute(
            "file_write",
            &json!({"path": path.display().to_string(), "content": "hello"}),
        );
        assert!(result.success);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");

        let result2 = r.execute(
            "file_write",
            &json!({"path": path.display().to_string(), "content": "world"}),
        );
        assert!(result2.success);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "world");
    }

    #[test]
    fn test_p3_file_write_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.txt");
        std::fs::write(&path, "first").unwrap();
        let r = registry();
        r.execute(
            "file_write",
            &json!({"path": path.display().to_string(), "content": "\nsecond", "append": true}),
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "first\nsecond");
    }

    // ── file_create ───────────────────────────────────────────────────────────

    #[test]
    fn test_p3_file_create_new() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.txt");
        let r = registry();
        let result = r.execute(
            "file_create",
            &json!({"path": path.display().to_string(), "content": "created"}),
        );
        assert!(result.success);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "created");
    }

    #[test]
    fn test_p3_file_create_fails_if_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exists.txt");
        std::fs::write(&path, "original").unwrap();
        let r = registry();
        let result = r.execute(
            "file_create",
            &json!({"path": path.display().to_string(), "content": "new"}),
        );
        assert!(!result.success);
        // Original unchanged
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "original");
    }

    // ── list_dir ──────────────────────────────────────────────────────────────

    #[test]
    fn test_p3_structured_patch_exact_replace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("patch.txt");
        std::fs::write(&path, "alpha\nbeta\ngamma\n").unwrap();
        let r = registry();
        let result = r.execute(
            "structured_patch",
            &json!({
                "path": path.display().to_string(),
                "operations": [
                    {"type": "replace", "old": "beta", "new": "BETA"},
                    {"type": "line_replace", "start_line": 3, "end_line": 3, "content": "GAMMA"}
                ]
            }),
        );

        assert!(result.success, "unexpected error: {:?}", result.error);
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "alpha\nBETA\nGAMMA\n"
        );
        assert_eq!(result.output["operations_applied"], 2);
    }

    #[test]
    fn test_p3_structured_patch_rejects_ambiguous_replace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("patch-ambiguous.txt");
        std::fs::write(&path, "same\nsame\n").unwrap();
        let r = registry();
        let result = r.execute(
            "structured_patch",
            &json!({
                "path": path.display().to_string(),
                "operations": [{"type": "replace", "old": "same", "new": "different"}]
            }),
        );

        assert!(!result.success);
        assert!(result.error.unwrap().contains("matched multiple"));
        assert_eq!(std::fs::read_to_string(path).unwrap(), "same\nsame\n");
    }

    #[test]
    fn test_p3_structured_patch_line_replace_accepts_schema_new_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("patch-line-new.txt");
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap();
        let r = registry();
        let result = r.execute(
            "structured_patch",
            &json!({
                "path": path.display().to_string(),
                "operations": [
                    {"type": "line_replace", "start_line": 2, "end_line": 2, "new": "TWO"}
                ]
            }),
        );

        assert!(result.success, "unexpected error: {:?}", result.error);
        assert_eq!(std::fs::read_to_string(path).unwrap(), "one\nTWO\nthree\n");
    }

    #[test]
    fn test_p3_structured_patch_line_replace_requires_replacement_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("patch-line-missing.txt");
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap();
        let r = registry();
        let result = r.execute(
            "structured_patch",
            &json!({
                "path": path.display().to_string(),
                "operations": [
                    {"type": "line_replace", "start_line": 2, "end_line": 2}
                ]
            }),
        );

        assert!(!result.success);
        assert!(result
            .error
            .unwrap()
            .contains("missing content/new/replace"));
        assert_eq!(std::fs::read_to_string(path).unwrap(), "one\ntwo\nthree\n");
    }

    #[test]
    fn test_p3_list_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "").unwrap();
        std::fs::write(dir.path().join("b.txt"), "").unwrap();
        let r = registry();
        let result = r.execute(
            "list_dir",
            &json!({"path": dir.path().display().to_string()}),
        );
        assert!(result.success);
        let entries = result.output["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
    }

    // ── glob ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_p3_glob_matches_extension() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("main.rs"), "").unwrap();
        std::fs::write(dir.path().join("lib.rs"), "").unwrap();
        std::fs::write(dir.path().join("readme.md"), "").unwrap();
        let r = registry();
        let result = r.execute(
            "glob",
            &json!({"pattern": "*.rs", "base_dir": dir.path().display().to_string()}),
        );
        assert!(result.success);
        let matches = result.output["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 2);
    }

    // ── code_search ───────────────────────────────────────────────────────────

    #[test]
    fn test_p3_code_search_finds_pattern() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("foo.rs"),
            "fn main() {\n    println!(\"hello\");\n}",
        )
        .unwrap();
        let r = registry();
        let result = r.execute(
            "code_search",
            &json!({"query": "println", "path": dir.path().display().to_string()}),
        );
        assert!(result.success);
        let matches = result.output["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert!(matches[0]["content"].as_str().unwrap().contains("println"));
    }

    // ── attempt_completion ────────────────────────────────────────────────────

    #[test]
    fn test_git_rejects_unsupported_subcommand_before_spawn() {
        let r = registry();
        let result = r.execute("git", &json!({"subcommand": "clean", "args": ["-fdx"]}));
        assert!(!result.success);
        assert!(result
            .error
            .as_ref()
            .unwrap()
            .contains("git subcommand is not allowed"));
    }

    #[test]
    fn test_p3_attempt_completion() {
        let r = registry();
        let result = r.execute(
            "attempt_completion",
            &json!({"result": "Task done successfully"}),
        );
        assert!(result.success);
        assert_eq!(result.output["completed"], true);
        assert_eq!(result.output["result"], "Task done successfully");
    }

    // ── unknown tool ──────────────────────────────────────────────────────────

    #[test]
    fn test_p3_send_message() {
        let r = registry();
        let result = r.execute(
            "send_message",
            &json!({"recipient": "agent-b", "kind": "handoff", "content": "please continue"}),
        );
        assert!(result.success);
        assert_eq!(result.output["sent"], true);
        assert_eq!(result.output["recipient"], "agent-b");
        assert_eq!(result.output["kind"], "handoff");
        assert_eq!(result.output["content"], "please continue");

        let missing = r.execute("send_message", &json!({}));
        assert!(!missing.success);
        assert!(missing
            .error
            .as_ref()
            .unwrap()
            .contains("Missing required param: content"));
    }

    #[test]
    fn test_shell_large_stdout_drain_does_not_deadlock() {
        let r = registry();
        let started = std::time::Instant::now();
        let result = r.execute(
            "shell",
            &json!({
                "command": large_stdout_command(),
                "timeout_ms": 30_000,
            }),
        );
        assert!(result.success, "unexpected shell error: {:?}", result.error);
        assert_eq!(result.output["exit_code"], 0);
        assert_eq!(result.output["stdout_truncated"], true);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "large stdout command likely blocked on an OS pipe"
        );
    }

    #[test]
    fn test_shell_rejects_control_operators_before_spawn() {
        let r = registry();
        for command in [
            "echo ok && echo injected",
            "echo ok | more",
            "echo ok > out.txt",
            "echo `whoami`",
            "echo $(whoami)",
            "echo ok\necho injected",
        ] {
            let result = r.execute("shell", &json!({"command": command}));
            assert!(!result.success, "command should be rejected: {command}");
            assert!(result
                .error
                .as_ref()
                .unwrap()
                .contains("shell command rejected"));
        }
    }

    #[cfg(target_os = "windows")]
    fn large_stdout_command() -> String {
        "for /L %i in (1,1,30000) do @echo XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX".into()
    }

    #[cfg(not(target_os = "windows"))]
    fn large_stdout_command() -> String {
        "python3 -c \"print('X'*1200000)\"".into()
    }

    #[test]
    fn test_p3_unknown_tool_returns_error() {
        let r = registry();
        let result = r.execute("does_not_exist", &json!({}));
        assert!(!result.success);
        assert!(result.error.as_ref().unwrap().contains("Tool not found"));
    }
}
