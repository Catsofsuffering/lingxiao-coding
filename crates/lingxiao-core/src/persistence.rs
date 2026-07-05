use rusqlite::{params, Connection, Result, Transaction, TransactionBehavior};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

pub const SCHEMA_VERSION: u32 = 19;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    pub workflows_paused: usize,
    pub agents_stopped: usize,
}

impl RecoveryReport {
    pub fn recovered_any(self) -> bool {
        self.workflows_paused > 0 || self.agents_stopped > 0
    }
}

const PRAGMA_INIT: &str = "\
    PRAGMA journal_mode = WAL;\
    PRAGMA busy_timeout = 30000;\
    PRAGMA foreign_keys = ON;\
    PRAGMA synchronous = NORMAL;\
    PRAGMA wal_autocheckpoint = 1000;\
    PRAGMA temp_store = MEMORY;\
";

const DDL_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        created_at REAL NOT NULL,
        workspace TEXT NOT NULL,
        user_request TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        summary TEXT,
        name TEXT
    );

    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        subject TEXT NOT NULL,
        description TEXT NOT NULL,
        context TEXT,
        status TEXT NOT NULL,
        exit_reason TEXT,
        run_generation INTEGER NOT NULL DEFAULT 0,
        agent_type TEXT NOT NULL,
        blocked_by TEXT,
        blocks TEXT,
        assigned_agent TEXT NOT NULL,
        preferred_agent_name TEXT,
        working_directory TEXT,
        write_scope TEXT,
        result TEXT,
        blocked_reason TEXT,
        orchestration TEXT,
        origin TEXT,
        goal TEXT,
        task_type TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (id, session_id)
    );

    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        sender TEXT NOT NULL,
        recipient TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS agent_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        agent_name TEXT NOT NULL,
        agent_role TEXT NOT NULL,
        task_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        content TEXT NOT NULL,
        token_usage TEXT,
        action TEXT,
        details TEXT,
        timestamp REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS agent_state (
        session_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        agent_name TEXT NOT NULL,
        agent_role TEXT NOT NULL,
        task_id TEXT NOT NULL,
        status TEXT NOT NULL,
        stopped INTEGER NOT NULL,
        iteration INTEGER NOT NULL,
        timestamp REAL NOT NULL,
        UNIQUE(session_id, agent_id)
    );

    CREATE TABLE IF NOT EXISTS session_state (
        session_id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        timestamp REAL NOT NULL,
        UNIQUE(session_id, key)
    );

    CREATE TABLE IF NOT EXISTS leader_conversation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        tool_calls TEXT,
        tool_call_id TEXT,
        thinking_blocks TEXT,
        timestamp REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS agent_conversation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        agent_name TEXT NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        tool_calls TEXT,
        tool_call_id TEXT,
        thinking_blocks TEXT,
        timestamp REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS token_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        agent_name TEXT NOT NULL,
        model_name TEXT NOT NULL,
        prompt_tokens INTEGER NOT NULL,
        completion_tokens INTEGER NOT NULL,
        total_tokens INTEGER NOT NULL,
        cache_read_tokens INTEGER DEFAULT 0,
        cache_creation_tokens INTEGER DEFAULT 0,
        timestamp REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS llm_gateway_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trace_id TEXT NOT NULL,
        session_id TEXT,
        agent_id TEXT,
        agent_name TEXT,
        key_id TEXT,
        key_label TEXT,
        profile TEXT,
        requested_model TEXT,
        selected_model TEXT,
        final_model TEXT,
        provider TEXT,
        status TEXT NOT NULL,
        prompt_tokens INTEGER DEFAULT 0,
        completion_tokens INTEGER DEFAULT 0,
        total_tokens INTEGER DEFAULT 0,
        cache_read_tokens INTEGER DEFAULT 0,
        cache_creation_tokens INTEGER DEFAULT 0,
        latency_ms INTEGER DEFAULT 0,
        attempts_json TEXT,
        error_kind TEXT,
        error_message TEXT,
        created_at REAL NOT NULL
    );

    -- traces defined only once (unlike TS where it appears in Database.ts and Tracing.ts)
    CREATE TABLE IF NOT EXISTS traces (
        trace_id TEXT NOT NULL,
        span_id TEXT PRIMARY KEY,
        parent_span_id TEXT,
        operation TEXT NOT NULL,
        start_ts INTEGER NOT NULL,
        end_ts INTEGER,
        status TEXT DEFAULT 'ok',
        attributes TEXT,
        session_id TEXT,
        agent_id TEXT
    );

    CREATE TABLE IF NOT EXISTS execution_trace_events (
        id TEXT PRIMARY KEY,
        project_root TEXT NOT NULL,
        session_id TEXT,
        task_id TEXT,
        agent_id TEXT,
        agent_name TEXT,
        agent_role TEXT,
        task_type TEXT,
        status TEXT NOT NULL,
        duration_ms INTEGER NOT NULL DEFAULT 0,
        files_changed TEXT NOT NULL DEFAULT '[]',
        error_signature TEXT,
        fix_pattern TEXT,
        verification TEXT,
        metadata TEXT,
        created_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS execution_project_models (
        project_root TEXT PRIMARY KEY,
        model_json TEXT NOT NULL,
        rebuilt_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS workflows (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        workspace TEXT,
        nodes TEXT,
        edges TEXT,
        version TEXT DEFAULT '1.0.0',
        config TEXT,
        tags TEXT,
        created_at REAL,
        updated_at REAL,
        created_by TEXT
    );

    CREATE TABLE IF NOT EXISTS workflow_executions (
        id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
        session_id TEXT NOT NULL,
        status TEXT NOT NULL,
        start_time INTEGER NOT NULL,
        end_time INTEGER,
        context TEXT,
        error TEXT,
        created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS workflow_execution_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id TEXT NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
        timestamp INTEGER NOT NULL,
        level TEXT NOT NULL,
        node_id TEXT,
        message TEXT NOT NULL,
        data TEXT
    );

    CREATE TABLE IF NOT EXISTS workflow_node_state (
        execution_id TEXT NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
        node_id TEXT NOT NULL,
        node_type TEXT NOT NULL,
        status TEXT NOT NULL,
        output_json TEXT,
        error TEXT,
        attempt INTEGER NOT NULL DEFAULT 0,
        generation INTEGER NOT NULL DEFAULT 1,
        started_at INTEGER,
        completed_at INTEGER,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (execution_id, node_id)
    );

    CREATE TABLE IF NOT EXISTS scheduled_tasks (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        cron TEXT NOT NULL,
        prompt TEXT NOT NULL,
        task_type TEXT NOT NULL DEFAULT 'prompt',
        intensity TEXT NOT NULL DEFAULT 'normal',
        audience TEXT NOT NULL DEFAULT 'personal',
        workflow_id TEXT,
        workflow_input TEXT,
        last_execution_id TEXT,
        last_error TEXT,
        source_type TEXT,
        source_id TEXT,
        source_node_id TEXT,
        recurring INTEGER NOT NULL DEFAULT 1,
        durable INTEGER NOT NULL DEFAULT 0,
        enabled INTEGER NOT NULL DEFAULT 1,
        last_run_at REAL,
        next_run_at REAL,
        created_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS health_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        timestamp REAL NOT NULL,
        source TEXT NOT NULL,
        has_critical INTEGER NOT NULL DEFAULT 0,
        decisions TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS worktrees (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        repo_root TEXT NOT NULL,
        path TEXT NOT NULL UNIQUE,
        branch TEXT NOT NULL,
        base_branch TEXT NOT NULL,
        session_id TEXT,
        task_id TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        last_error TEXT
    );

    CREATE TABLE IF NOT EXISTS graph_nodes (
        id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        tags TEXT NOT NULL DEFAULT '[]',
        created_by TEXT NOT NULL,
        created_at REAL NOT NULL,
        superseded_by TEXT,
        confidence TEXT,
        intent_status TEXT,
        priority INTEGER,
        evidence TEXT,
        intent_from TEXT,
        intent_to TEXT,
        contract_allowed_scope TEXT,
        PRIMARY KEY (id, session_id)
    );

    CREATE TABLE IF NOT EXISTS graph_edges (
        id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        from_node_id TEXT NOT NULL,
        to_node_id TEXT NOT NULL,
        edge_type TEXT NOT NULL,
        created_at REAL NOT NULL,
        created_by TEXT NOT NULL,
        metadata TEXT,
        PRIMARY KEY (id, session_id)
    );

    CREATE TABLE IF NOT EXISTS assumptions (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        content TEXT,
        status TEXT NOT NULL DEFAULT 'unverified',
        verification_type TEXT NOT NULL,
        verification_target TEXT NOT NULL,
        verification_expected TEXT NOT NULL,
        verification_actual TEXT,
        dependents TEXT NOT NULL DEFAULT '[]',
        created_by TEXT,
        created_at REAL NOT NULL,
        verified_at REAL,
        falsified_at REAL,
        evidence TEXT,
        session_id TEXT
    );

    CREATE TABLE IF NOT EXISTS tool_registrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        tool_type TEXT NOT NULL DEFAULT 'class',
        tool_description TEXT NOT NULL DEFAULT '',
        tool_schema TEXT NOT NULL DEFAULT '{}',
        registered_at REAL NOT NULL,
        UNIQUE(session_id, tool_name)
    );

    -- Rust Core tool-call ledger: canonical tool/sidecar state and resource accounting.
    CREATE TABLE IF NOT EXISTS tool_calls (
        id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        tool_type TEXT NOT NULL DEFAULT 'native',
        status TEXT NOT NULL,
        args_json TEXT NOT NULL DEFAULT '{}',
        result_json TEXT,
        error TEXT,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        cancelled_at INTEGER,
        resource_usage_json TEXT NOT NULL DEFAULT '{}',
        PRIMARY KEY (session_id, id)
    );

    CREATE TABLE IF NOT EXISTS owned_processes (
        id TEXT PRIMARY KEY,
        pid INTEGER NOT NULL,
        owner_kind TEXT NOT NULL,
        owner_id TEXT NOT NULL,
        label TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at REAL NOT NULL,
        completed_at REAL,
        cleanup_attempted_at REAL,
        exit_code INTEGER,
        last_error TEXT
    );

    CREATE TABLE IF NOT EXISTS terminal_sessions (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        pid INTEGER NOT NULL,
        shell TEXT NOT NULL,
        cwd TEXT,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        last_activity_at REAL NOT NULL,
        completed_at REAL,
        exit_code INTEGER,
        last_error TEXT
    );

    CREATE TABLE IF NOT EXISTS teams (
        name TEXT NOT NULL,
        description TEXT,
        leader_name TEXT NOT NULL,
        members_json TEXT NOT NULL DEFAULT '[]',
        workspace TEXT NOT NULL,
        session_id TEXT NOT NULL,
        created_at REAL NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (session_id, name)
    );

    CREATE TABLE IF NOT EXISTS team_members (
        name TEXT NOT NULL,
        team TEXT NOT NULL,
        role TEXT NOT NULL,
        workspace TEXT NOT NULL,
        session_id TEXT NOT NULL,
        registered_at REAL NOT NULL,
        PRIMARY KEY (session_id, name)
    );

    CREATE TABLE IF NOT EXISTS team_messages (
        id TEXT PRIMARY KEY,
        from_team TEXT NOT NULL,
        from_member TEXT,
        to_team TEXT NOT NULL,
        to_member TEXT,
        content TEXT NOT NULL,
        urgency TEXT NOT NULL DEFAULT 'normal',
        kind TEXT NOT NULL DEFAULT 'normal',
        request_id TEXT,
        session_id TEXT NOT NULL,
        timestamp REAL NOT NULL,
        read_by TEXT NOT NULL DEFAULT '[]',
        metadata TEXT
    );

    -- memory_entry (from MemoryFTS.ts scope; merged into core DB in Rust)
    CREATE TABLE IF NOT EXISTS memory_entry (
        id TEXT PRIMARY KEY,
        path TEXT NOT NULL UNIQUE,
        scope TEXT NOT NULL,
        scope_id TEXT NOT NULL,
        type TEXT NOT NULL,
        body TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        last_indexed_at INTEGER NOT NULL
    );

    CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
        path UNINDEXED,
        scope UNINDEXED,
        scope_id UNINDEXED,
        type UNINDEXED,
        body
    );

    -- memory_embedding: explicitly defined (missing from TS MemoryFTS.ts — see docs/contracts/rust-core-sqlite-schema-map.md §2.10)
    -- Phase 2 will determine standalone vs merged memory DB; for P1 schema parity we define it in core DB.
    CREATE TABLE IF NOT EXISTS memory_embedding (
        path TEXT PRIMARY KEY,
        embedding BLOB NOT NULL,
        model TEXT NOT NULL,
        dimensions INTEGER NOT NULL,
        created_at INTEGER NOT NULL
    );

    -- Event log (Rust Core addition — ordered durable event store for command sourcing + recovery)
    -- Columns mirror EventEnvelope; event_id is globally UNIQUE for idempotency;
    -- (session_id, seq) is the logical primary key for per-session ordering.
    CREATE TABLE IF NOT EXISTS event_log (
        event_id TEXT NOT NULL UNIQUE,
        session_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        generation INTEGER NOT NULL DEFAULT 1,
        event_type TEXT NOT NULL,
        source_kind TEXT NOT NULL,
        source_id TEXT,
        payload TEXT NOT NULL DEFAULT '{}',
        occurred_at INTEGER NOT NULL,
        causation_id TEXT,
        correlation_id TEXT,
        PRIMARY KEY (session_id, seq)
    );

    CREATE TABLE IF NOT EXISTS event_log_meta (
        session_id TEXT PRIMARY KEY,
        last_seq INTEGER NOT NULL DEFAULT 0,
        current_generation INTEGER NOT NULL DEFAULT 1,
        compacted_seq INTEGER NOT NULL DEFAULT 0
    );

    -- Command-level idempotency dedup table (composite PK: key + method)
    CREATE TABLE IF NOT EXISTS command_dedupe (
        idempotency_key TEXT NOT NULL,
        method TEXT NOT NULL,
        response_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (idempotency_key, method)
    );

    CREATE TABLE IF NOT EXISTS provider_health (
        provider_id TEXT PRIMARY KEY,
        failure_count INTEGER NOT NULL DEFAULT 0,
        last_failure_ms INTEGER NOT NULL DEFAULT 0,
        circuit_open INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS permission_modes (
        session_id TEXT PRIMARY KEY,
        mode TEXT NOT NULL,
        generation INTEGER NOT NULL DEFAULT 1,
        updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS permission_requests (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        args_json TEXT NOT NULL DEFAULT '{}',
        mode TEXT NOT NULL,
        status TEXT NOT NULL,
        decision TEXT,
        reason TEXT,
        created_at INTEGER NOT NULL,
        resolved_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS permission_grants (
        session_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        mode TEXT NOT NULL,
        scope TEXT,
        granted_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, tool_name)
    );
";

const DDL_INDEXES: &str = "
    CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_session ON tasks(session_id);
    CREATE INDEX IF NOT EXISTS idx_agent_logs_session ON agent_logs(session_id);
    CREATE INDEX IF NOT EXISTS idx_agent_state_session ON agent_state(session_id);
    CREATE INDEX IF NOT EXISTS idx_leader_conv_session ON leader_conversation(session_id);
    CREATE INDEX IF NOT EXISTS idx_agent_conv_session ON agent_conversation(session_id, agent_id);
    CREATE INDEX IF NOT EXISTS idx_token_usage_session ON token_usage(session_id);
    CREATE INDEX IF NOT EXISTS idx_llm_gateway_trace ON llm_gateway_requests(trace_id);
    CREATE INDEX IF NOT EXISTS idx_llm_gateway_session ON llm_gateway_requests(session_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_llm_gateway_key ON llm_gateway_requests(key_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_traces_trace ON traces(trace_id);
    CREATE INDEX IF NOT EXISTS idx_traces_session ON traces(session_id, start_ts);
    CREATE INDEX IF NOT EXISTS idx_execution_trace_project ON execution_trace_events(project_root, created_at);
    CREATE INDEX IF NOT EXISTS idx_execution_trace_task ON execution_trace_events(session_id, task_id);
    CREATE INDEX IF NOT EXISTS idx_execution_trace_status ON execution_trace_events(project_root, status);
    CREATE INDEX IF NOT EXISTS idx_workflow_executions_workflow ON workflow_executions(workflow_id);
    CREATE INDEX IF NOT EXISTS idx_workflow_executions_session ON workflow_executions(session_id);
    CREATE INDEX IF NOT EXISTS idx_workflow_logs_execution ON workflow_execution_logs(execution_id);
    CREATE INDEX IF NOT EXISTS idx_workflow_logs_timestamp ON workflow_execution_logs(timestamp);
    CREATE INDEX IF NOT EXISTS idx_workflow_node_state_status ON workflow_node_state(execution_id, status);
    CREATE INDEX IF NOT EXISTS idx_workflow_node_state_updated ON workflow_node_state(execution_id, updated_at);
    CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_session ON scheduled_tasks(session_id);
    CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_next ON scheduled_tasks(next_run_at);
    CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_workflow ON scheduled_tasks(workflow_id) WHERE workflow_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_source ON scheduled_tasks(source_type, source_id, source_node_id) WHERE source_type IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_hr_session_ts ON health_reports(session_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_worktrees_session ON worktrees(session_id);
    CREATE INDEX IF NOT EXISTS idx_worktrees_repo ON worktrees(repo_root);
    CREATE INDEX IF NOT EXISTS idx_worktrees_status ON worktrees(status);
    CREATE INDEX IF NOT EXISTS idx_graph_nodes_session ON graph_nodes(session_id);
    CREATE INDEX IF NOT EXISTS idx_graph_nodes_kind ON graph_nodes(session_id, kind);
    CREATE INDEX IF NOT EXISTS idx_graph_nodes_status ON graph_nodes(session_id, intent_status);
    CREATE INDEX IF NOT EXISTS idx_graph_edges_from ON graph_edges(session_id, from_node_id);
    CREATE INDEX IF NOT EXISTS idx_graph_edges_to ON graph_edges(session_id, to_node_id);
    CREATE INDEX IF NOT EXISTS idx_graph_edges_type ON graph_edges(session_id, edge_type);
    CREATE INDEX IF NOT EXISTS idx_assumptions_status ON assumptions(status, session_id);
    CREATE INDEX IF NOT EXISTS idx_assumptions_target ON assumptions(session_id, verification_target);
    CREATE INDEX IF NOT EXISTS idx_tool_registrations_session ON tool_registrations(session_id);
    CREATE INDEX IF NOT EXISTS idx_tool_registrations_name ON tool_registrations(tool_name);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_session ON tool_calls(session_id);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_status ON tool_calls(session_id, status);
    CREATE INDEX IF NOT EXISTS idx_teams_session ON teams(session_id);
    CREATE INDEX IF NOT EXISTS idx_team_members_team ON team_members(team);
    CREATE INDEX IF NOT EXISTS idx_team_members_session ON team_members(session_id);
    CREATE INDEX IF NOT EXISTS idx_team_messages_to_team ON team_messages(to_team);
    CREATE INDEX IF NOT EXISTS idx_team_messages_to_member ON team_messages(to_member);
    CREATE INDEX IF NOT EXISTS idx_team_messages_session ON team_messages(session_id);
    CREATE INDEX IF NOT EXISTS memory_scope_idx ON memory_entry(scope, scope_id);
    CREATE INDEX IF NOT EXISTS memory_type_idx ON memory_entry(type);
    CREATE INDEX IF NOT EXISTS idx_event_log_session ON event_log(session_id, seq);
";

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub path: String,
    pub busy_timeout_ms: u32,
    pub wal_autocheckpoint: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            path: "lingxiao.db".into(),
            busy_timeout_ms: 30000,
            wal_autocheckpoint: 1000,
        }
    }
}

pub struct DbOwner {
    conn: Arc<Mutex<Connection>>,
}

impl Clone for DbOwner {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
        }
    }
}

impl DbOwner {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn initialize(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(PRAGMA_INIT)?;
        conn.execute_batch(DDL_TABLES)?;
        conn.execute_batch(DDL_INDEXES)?;
        ensure_schema_upgrades(&conn)?;
        conn.execute_batch(&format!("PRAGMA user_version = {};", SCHEMA_VERSION))?;
        Ok(())
    }

    /// Reset stuck runtime state after crash. Opt-in via explicit call.
    ///
    /// - Workflows stuck 'running' → 'paused'
    /// - Sessions stuck 'active' remain active (event replay handles recovery)
    /// - Tasks stuck 'running' remain running (late-result rejection handles stale completions)
    /// - Agents stuck 'running' → 'stopped'
    pub fn recover_orphans(&self) -> Result<RecoveryReport> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        // Workflow recovery: running → paused (GS-016/028 pattern)
        let workflow_count = tx.execute(
            "UPDATE workflow_executions SET status = 'paused' WHERE status = 'running'",
            [],
        )?;

        // Agent recovery: running → stopped (set stopped=1, update timestamp)
        let agent_count = tx.execute(
            "UPDATE agent_state SET status = 'stopped', stopped = 1, timestamp = ? \
             WHERE status = 'running'",
            params![now_ts],
        )?;

        tx.commit()?;
        let report = RecoveryReport {
            workflows_paused: workflow_count,
            agents_stopped: agent_count,
        };

        if report.recovered_any() {
            eprintln!(
                "[recovery] Reset {} stuck workflows, {} stuck agents",
                report.workflows_paused, report.agents_stopped
            );
        }

        Ok(report)
    }

    pub fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }

    pub fn with_transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Transaction) -> Result<T>,
    {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }
}

fn ensure_schema_upgrades(conn: &Connection) -> Result<()> {
    if let Err(error) = conn.execute(
        "ALTER TABLE event_log_meta ADD COLUMN compacted_seq INTEGER NOT NULL DEFAULT 0",
        [],
    ) {
        let message = error.to_string();
        if !message.contains("duplicate column name") {
            return Err(error);
        }
    }
    conn.execute(
        "CREATE TABLE IF NOT EXISTS owned_processes (
            id TEXT PRIMARY KEY,
            pid INTEGER NOT NULL,
            owner_kind TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            label TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at REAL NOT NULL,
            completed_at REAL,
            cleanup_attempted_at REAL,
            exit_code INTEGER,
            last_error TEXT
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS terminal_sessions (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            pid INTEGER NOT NULL,
            shell TEXT NOT NULL,
            cwd TEXT,
            status TEXT NOT NULL,
            created_at REAL NOT NULL,
            last_activity_at REAL NOT NULL,
            completed_at REAL,
            exit_code INTEGER,
            last_error TEXT
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS provider_health (
            provider_id TEXT PRIMARY KEY,
            failure_count INTEGER NOT NULL DEFAULT 0,
            last_failure_ms INTEGER NOT NULL DEFAULT 0,
            circuit_open INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )",
        [],
    )?;
    if let Err(error) = conn.execute("ALTER TABLE permission_grants ADD COLUMN scope TEXT", []) {
        let message = error.to_string();
        if !message.contains("duplicate column name") {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn create_test_db() -> DbOwner {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        db
    }

    #[test]
    fn test_schema_version_constant() {
        assert_eq!(SCHEMA_VERSION, 19);
    }

    #[test]
    fn test_default_config() {
        let config = DbConfig::default();
        assert_eq!(config.busy_timeout_ms, 30000);
        assert_eq!(config.wal_autocheckpoint, 1000);
    }

    #[test]
    fn test_pragma_user_version() {
        let db = create_test_db();
        let conn = db.conn();
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn test_pragma_journal_mode() {
        let dir = std::env::temp_dir().join("lingxiao_test_journal");
        let _ = std::fs::create_dir_all(&dir);
        let db_path = dir.join("test.db");
        let _ = std::fs::remove_file(&db_path);
        let db = DbOwner::open(&db_path).unwrap();
        db.initialize().unwrap();
        let conn = db.conn();
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_pragma_foreign_keys() {
        let db = create_test_db();
        let conn = db.conn();
        let fk: i32 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        assert_eq!(fk, 1);
    }

    #[test]
    fn test_pragma_busy_timeout() {
        let db = create_test_db();
        let conn = db.conn();
        let timeout: i32 = conn
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert_eq!(timeout, 30000);
    }

    #[test]
    fn test_pragma_synchronous() {
        let db = create_test_db();
        let conn = db.conn();
        let sync: i32 = conn
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(sync, 1);
    }

    #[test]
    fn test_pragma_temp_store() {
        let db = create_test_db();
        let conn = db.conn();
        let store: i32 = conn
            .pragma_query_value(None, "temp_store", |row| row.get(0))
            .unwrap();
        assert_eq!(store, 2);
    }

    #[test]
    fn test_pragma_wal_autocheckpoint() {
        let db = create_test_db();
        let conn = db.conn();
        let checkpoint: i32 = conn
            .pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
            .unwrap();
        assert_eq!(checkpoint, 1000);
    }

    #[test]
    fn test_p0_tables_exist() {
        let db = create_test_db();
        let conn = db.conn();
        let required_tables = [
            "sessions",
            "tasks",
            "messages",
            "leader_conversation",
            "event_log",
            "event_log_meta",
        ];
        for table in &required_tables {
            let count: i32 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    params![table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "Required P0 table '{}' does not exist", table);
        }
    }

    #[test]
    fn test_all_tables_exist() {
        let db = create_test_db();
        let conn = db.conn();
        let expected_tables = [
            "sessions",
            "tasks",
            "messages",
            "agent_logs",
            "agent_state",
            "session_state",
            "leader_conversation",
            "agent_conversation",
            "token_usage",
            "llm_gateway_requests",
            "traces",
            "execution_trace_events",
            "execution_project_models",
            "workflows",
            "workflow_executions",
            "workflow_execution_logs",
            "workflow_node_state",
            "scheduled_tasks",
            "health_reports",
            "worktrees",
            "graph_nodes",
            "graph_edges",
            "assumptions",
            "tool_registrations",
            "tool_calls",
            "owned_processes",
            "terminal_sessions",
            "teams",
            "team_members",
            "team_messages",
            "memory_entry",
            "memory_fts",
            "memory_embedding",
            "event_log",
            "event_log_meta",
            "command_dedupe",
            "permission_modes",
            "permission_requests",
            "permission_grants",
        ];
        for table in &expected_tables {
            let count: i32 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    params![table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "Expected table '{}' does not exist", table);
        }
    }

    #[test]
    fn test_traces_single_definition() {
        let db = create_test_db();
        let conn = db.conn();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='traces'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "traces table must be defined exactly once");

        let index_info: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='traces' AND name LIKE 'idx_traces_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            index_info, 2,
            "traces should have exactly 2 indexes (idx_traces_trace, idx_traces_session)"
        );
    }

    #[test]
    fn test_memory_embedding_exists() {
        let db = create_test_db();
        let conn = db.conn();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='memory_embedding'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "memory_embedding table must be explicitly defined"
        );
        let col_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('memory_embedding')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(col_count, 5, "memory_embedding should have 5 columns: path, embedding, model, dimensions, created_at");
    }

    #[test]
    fn test_transaction_rollback() {
        let db = create_test_db();
        let result: std::result::Result<(), rusqlite::Error> = db.with_transaction(|tx| {
            tx.execute(
                "INSERT INTO sessions (id, created_at, workspace, status) VALUES (?1, ?2, ?3, ?4)",
                params!["rollback-test", 1000.0, "/tmp", "active"],
            )?;
            Err(rusqlite::Error::InvalidParameterName(
                "force_rollback".into(),
            ))
        });
        assert!(result.is_err());
        let conn = db.conn();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id='rollback-test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 0,
            "Transaction rollback: row should not exist after failed tx"
        );
    }

    #[test]
    fn test_transaction_commit() {
        let db = create_test_db();
        let result = db.with_transaction(|tx| {
            tx.execute(
                "INSERT INTO sessions (id, created_at, workspace, status) VALUES (?1, ?2, ?3, ?4)",
                params!["commit-test", 2000.0, "/tmp", "active"],
            )?;
            Ok(())
        });
        assert!(result.is_ok());
        let conn = db.conn();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id='commit-test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "Transaction commit: row should exist after successful tx"
        );
    }

    #[test]
    fn test_init_idempotent() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let count_after_first: i32 = {
            let conn = db.conn();
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        db.initialize().unwrap();
        let count_after_second: i32 = {
            let conn = db.conn();
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(
            count_after_first, count_after_second,
            "Idempotent init should not duplicate tables"
        );
    }

    #[test]
    fn test_external_writer_blocked_during_transaction() {
        let db_path = std::env::temp_dir().join("lingxiao_test_external_lock.db");
        let _ = std::fs::remove_file(&db_path);
        let db = DbOwner::open(&db_path).unwrap();
        db.initialize().unwrap();

        let conn = db.conn();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.execute(
            "INSERT INTO sessions (id, created_at, workspace, status) VALUES (?1, ?2, ?3, ?4)",
            params!["locked-session", 1000.0, "/tmp", "active"],
        )
        .unwrap();

        let external = Connection::open(&db_path).unwrap();
        external.execute_batch("PRAGMA busy_timeout = 100").unwrap();
        let ext_result = external.execute(
            "INSERT INTO sessions (id, created_at, workspace, status) VALUES (?1, ?2, ?3, ?4)",
            params!["external-session", 2000.0, "/tmp", "active"],
        );

        assert!(
            ext_result.is_err(),
            "External writer should get SQLITE_BUSY (or timeout error) while core holds IMMEDIATE transaction"
        );

        conn.execute_batch("ROLLBACK").unwrap();
        let _ = std::fs::remove_file(&db_path);
    }

    #[test]
    fn test_session_schema() {
        let db = create_test_db();
        let conn = db.conn();
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('sessions')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"created_at".to_string()));
        assert!(cols.contains(&"workspace".to_string()));
        assert!(cols.contains(&"status".to_string()));
    }

    #[test]
    fn test_task_schema() {
        let db = create_test_db();
        let conn = db.conn();
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('tasks')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"session_id".to_string()));
        assert!(cols.contains(&"subject".to_string()));
        assert!(cols.contains(&"status".to_string()));
        assert!(cols.contains(&"run_generation".to_string()));
        assert!(cols.contains(&"blocked_by".to_string()));
        assert!(cols.contains(&"blocks".to_string()));
    }

    #[test]
    fn test_task_working_directory_nullable() {
        let db = create_test_db();
        let conn = db.conn();
        let (notnull, dflt_value): (i32, Option<String>) = conn
            .query_row(
                "SELECT `notnull`, `dflt_value` FROM pragma_table_info('tasks') WHERE name='working_directory'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(notnull, 0, "working_directory must be nullable (notnull=0)");
        assert_eq!(
            dflt_value, None,
            "working_directory must have no DEFAULT (dflt_value=NULL)"
        );
    }

    #[test]
    fn test_recover_orphans_returns_structured_counts() {
        let db = create_test_db();
        {
            let conn = db.conn();
            conn.execute(
                "INSERT INTO sessions (id, created_at, workspace, status) \
                 VALUES ('recover-sess', 1, '/tmp', 'active')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO workflows (id, name) VALUES ('recover-wf', 'recover-wf')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO workflow_executions \
                 (id, workflow_id, session_id, status, start_time, created_at) \
                 VALUES ('recover-exec', 'recover-wf', 'recover-sess', 'running', 1, 1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO agent_state \
                 (session_id, agent_id, agent_name, agent_role, task_id, status, stopped, iteration, timestamp) \
                 VALUES ('recover-sess', 'recover-agent', 'Recover Agent', 'worker', 'task', 'running', 0, 0, 1)",
                [],
            )
            .unwrap();
        }

        let report = db.recover_orphans().unwrap();
        assert_eq!(
            report,
            RecoveryReport {
                workflows_paused: 1,
                agents_stopped: 1,
            }
        );
        assert!(report.recovered_any());

        let conn = db.conn();
        let workflow_status: String = conn
            .query_row(
                "SELECT status FROM workflow_executions WHERE id = 'recover-exec'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let (agent_status, stopped): (String, i64) = conn
            .query_row(
                "SELECT status, stopped FROM agent_state WHERE agent_id = 'recover-agent'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(workflow_status, "paused");
        assert_eq!(agent_status, "stopped");
        assert_eq!(stopped, 1);
    }

    #[test]
    fn test_task_blocked_by_and_blocks_nullable() {
        let db = create_test_db();
        let conn = db.conn();
        for col in &["blocked_by", "blocks", "write_scope"] {
            let (notnull,): (i32,) = conn
                .query_row(
                    "SELECT `notnull` FROM pragma_table_info('tasks') WHERE name=?1",
                    params![col],
                    |row| Ok((row.get(0)?,)),
                )
                .unwrap();
            assert_eq!(
                notnull, 0,
                "tasks.{} must be nullable (notnull=0) per schema map 约束 column",
                col
            );
        }
    }

    #[test]
    fn test_db_owner_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<DbOwner>();
        assert_sync::<DbOwner>();
    }

    #[test]
    fn test_event_log_schema() {
        let db = create_test_db();
        let conn = db.conn();
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('event_log')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(cols.contains(&"event_id".to_string()));
        assert!(cols.contains(&"session_id".to_string()));
        assert!(cols.contains(&"seq".to_string()));
        assert!(cols.contains(&"generation".to_string()));
        assert!(cols.contains(&"event_type".to_string()));
        assert!(cols.contains(&"source_kind".to_string()));
        assert!(cols.contains(&"source_id".to_string()));
        assert!(cols.contains(&"payload".to_string()));
        assert!(cols.contains(&"occurred_at".to_string()));
        assert!(cols.contains(&"causation_id".to_string()));
        assert!(cols.contains(&"correlation_id".to_string()));
    }

    #[test]
    fn test_event_log_meta_schema() {
        let db = create_test_db();
        let conn = db.conn();
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('event_log_meta')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(cols.contains(&"session_id".to_string()));
        assert!(cols.contains(&"last_seq".to_string()));
        assert!(cols.contains(&"current_generation".to_string()));
    }

    #[test]
    fn test_ddl_contains_single_traces_create() {
        let traces_count = DDL_TABLES
            .matches("CREATE TABLE IF NOT EXISTS traces")
            .count();
        assert_eq!(
            traces_count, 1,
            "traces DDL must be defined exactly once in DDL_TABLES"
        );
    }

    #[test]
    fn test_memory_embedding_comment() {
        assert!(
            DDL_TABLES
                .contains("memory_embedding: explicitly defined (missing from TS MemoryFTS.ts"),
            "memory_embedding must have a code comment explaining it was missing from TS"
        );
    }
}
