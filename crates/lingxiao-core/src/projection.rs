use crate::event_log::{EventLog, MAX_REPLAY_EVENTS};
use lingxiao_core_protocol::event::EventEnvelope;
use lingxiao_core_protocol::snapshot::SnapshotEnvelope;
use lingxiao_core_protocol::types::*;
use rusqlite::{params, OptionalExtension, Result};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

const SNAPSHOT_COLLECTION_LIMIT: i64 = 500;

/// Outcome of a [`ProjectionService::connect`] call.
#[derive(Debug, Clone)]
pub enum ConnectResult {
    /// Delta events for the client (gap ≤ MAX_REPLAY_EVENTS).
    Delta {
        events: Vec<EventEnvelope>,
        latest_seq: Seq,
    },
    /// Gap too large or cursor ahead of server — client should request a snapshot.
    SnapshotRequired { message: String, latest_seq: Seq },
}

/// Minimal projection service: cursor-based reconnect and snapshot building.
#[derive(Clone)]
pub struct ProjectionService {
    event_log: EventLog,
}

impl ProjectionService {
    pub fn new(event_log: EventLog) -> Self {
        Self { event_log }
    }

    /// Handle a reconnect cursor.
    ///
    /// Returns:
    /// - [`ConnectResult::Delta`] with events `seq > cursor.last_known_seq`
    ///   when the gap ≤ [`MAX_REPLAY_EVENTS`].
    /// - [`ConnectResult::SnapshotRequired`] when the gap exceeds the threshold
    ///   or when `cursor.last_known_seq > latest_seq` (client ahead of server).
    pub fn connect(&self, cursor: &ReplayCursor) -> Result<ConnectResult> {
        let latest_seq = self.event_log.latest_seq(&cursor.session_id)?;
        let cursor_seq = cursor.last_known_seq;
        let compacted_seq = self.event_log.compacted_seq(&cursor.session_id)?;

        if cursor_seq > latest_seq {
            return Ok(ConnectResult::SnapshotRequired {
                message: format!(
                    "Client cursor seq {cursor_seq} is ahead of server latest seq {latest_seq}. \
                     Request full snapshot."
                ),
                latest_seq,
            });
        }

        if cursor_seq < compacted_seq {
            return Ok(ConnectResult::SnapshotRequired {
                message: format!(
                    "Cursor seq {cursor_seq} is before compacted seq {compacted_seq}. \
                     Request full snapshot."
                ),
                latest_seq,
            });
        }

        let gap = latest_seq.saturating_sub(cursor_seq);

        if gap > MAX_REPLAY_EVENTS {
            return Ok(ConnectResult::SnapshotRequired {
                message: format!(
                    "Gap too large ({gap} > {MAX_REPLAY_EVENTS}). Request full snapshot."
                ),
                latest_seq,
            });
        }

        let batch = self
            .event_log
            .replay(&cursor.session_id, cursor_seq, MAX_REPLAY_EVENTS)?;

        Ok(ConnectResult::Delta {
            events: batch.events,
            latest_seq,
        })
    }

    /// Build the canonical headless Rust projection for the given session.
    ///
    /// Rust Core does not own TUI/Web/Electron view state. The canonical
    /// projection is the headless runtime contract: session status, pending
    /// permissions, active context, tasks, agents, workflows, and tool-call
    /// summaries. UI adapters fold these stable fields into their own views.
    pub fn snapshot(&self, session_id: &str) -> Result<SnapshotEnvelope> {
        let latest_seq = self.event_log.latest_seq(session_id)?;
        let generation = self.event_log.current_generation(session_id)?;

        let conn = self.event_log.db.conn();
        let status: String = conn
            .query_row(
                "SELECT COALESCE(status, 'unknown') FROM sessions WHERE id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_else(|| "unknown".to_string());

        let permission_mode: String = conn
            .query_row(
                "SELECT mode FROM permission_modes WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_else(|| "strict".to_string());

        let pending_permissions = {
            let mut stmt = conn.prepare(
                "SELECT id, tool_name, args_json, mode, created_at \
                 FROM permission_requests \
                 WHERE session_id = ?1 AND status = 'pending' \
                 ORDER BY created_at, id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![session_id, SNAPSHOT_COLLECTION_LIMIT], |row| {
                let args_json: String = row.get(2)?;
                let args: serde_json::Value =
                    serde_json::from_str(&args_json).unwrap_or_else(|_| json!({}));
                Ok(json!({
                    "permission_request_id": row.get::<_, String>(0)?,
                    "tool_name": row.get::<_, String>(1)?,
                    "args": args,
                    "mode": row.get::<_, String>(3)?,
                    "created_at": row.get::<_, i64>(4)?,
                }))
            })?;
            rows.collect::<Result<Vec<serde_json::Value>>>()?
        };

        let active_context = conn
            .query_row(
                "SELECT value FROM session_state \
                 WHERE session_id = ?1 AND key = 'active_context_projection'",
                params![session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok());

        let tasks = {
            let mut stmt = conn.prepare(
                "SELECT id, subject, description, status, agent_type, assigned_agent, created_at, updated_at \
                 FROM tasks WHERE session_id = ?1 ORDER BY created_at, id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![session_id, SNAPSHOT_COLLECTION_LIMIT], |row| {
                Ok(json!({
                    "task_id": row.get::<_, String>(0)?,
                    "subject": row.get::<_, String>(1)?,
                    "description": row.get::<_, String>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "agent_type": row.get::<_, String>(4)?,
                    "assigned_agent": row.get::<_, String>(5)?,
                    "created_at": row.get::<_, f64>(6)?,
                    "updated_at": row.get::<_, f64>(7)?,
                }))
            })?;
            rows.collect::<Result<Vec<serde_json::Value>>>()?
        };

        let agents = {
            let mut stmt = conn.prepare(
                "SELECT agent_id, agent_name, agent_role, status, task_id, iteration, timestamp \
                 FROM agent_state WHERE session_id = ?1 ORDER BY timestamp, agent_id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![session_id, SNAPSHOT_COLLECTION_LIMIT], |row| {
                Ok(json!({
                    "agent_id": row.get::<_, String>(0)?,
                    "agent_name": row.get::<_, String>(1)?,
                    "agent_role": row.get::<_, String>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "task_id": row.get::<_, String>(4)?,
                    "iteration": row.get::<_, i64>(5)?,
                    "timestamp": row.get::<_, f64>(6)?,
                }))
            })?;
            rows.collect::<Result<Vec<serde_json::Value>>>()?
        };

        let workflows = {
            let mut stmt = conn.prepare(
                "SELECT id, workflow_id, status, start_time, end_time, error \
                 FROM workflow_executions WHERE session_id = ?1 ORDER BY start_time, id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![session_id, SNAPSHOT_COLLECTION_LIMIT], |row| {
                Ok(json!({
                    "execution_id": row.get::<_, String>(0)?,
                    "workflow_id": row.get::<_, String>(1)?,
                    "status": row.get::<_, String>(2)?,
                    "start_time": row.get::<_, i64>(3)?,
                    "end_time": row.get::<_, Option<i64>>(4)?,
                    "error": row.get::<_, Option<String>>(5)?,
                }))
            })?;
            rows.collect::<Result<Vec<serde_json::Value>>>()?
        };

        let tool_calls = {
            let mut stmt = conn.prepare(
                "SELECT id, tool_name, tool_type, status, started_at, completed_at, error \
                 FROM tool_calls WHERE session_id = ?1 ORDER BY started_at, id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![session_id, SNAPSHOT_COLLECTION_LIMIT], |row| {
                Ok(json!({
                    "tool_call_id": row.get::<_, String>(0)?,
                    "tool_name": row.get::<_, String>(1)?,
                    "tool_type": row.get::<_, String>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "started_at": row.get::<_, i64>(4)?,
                    "completed_at": row.get::<_, Option<i64>>(5)?,
                    "error": row.get::<_, Option<String>>(6)?,
                }))
            })?;
            rows.collect::<Result<Vec<serde_json::Value>>>()?
        };

        let payload = json!({
            "snapshot_seq": latest_seq,
            "generation": generation,
            "permission_mode": permission_mode,
            "pending_permissions": pending_permissions,
            "active_context": active_context,
            "headless_projection": {
                "boundary": "rust-core-headless",
                "collection_limit": SNAPSHOT_COLLECTION_LIMIT,
                "tasks": tasks,
                "agents": agents,
                "workflows": workflows,
                "tool_calls": tool_calls,
                "redaction": {
                    "message_bodies": "omitted",
                    "tool_args": "omitted",
                    "tool_results": "omitted",
                    "auth_context": "omitted"
                }
            },
        });

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as Timestamp;

        Ok(SnapshotEnvelope {
            snapshot_id: None,
            session_id: session_id.to_string(),
            generation,
            last_seq: latest_seq,
            status,
            payload,
            created_at,
        })
    }

    /// Expose the inner [`EventLog`] for direct log operations.
    pub fn event_log(&self) -> &EventLog {
        &self.event_log
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::DbOwner;
    use lingxiao_core_protocol::actor::{Actor, ActorKind};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_event(
        session_id: &str,
        event_type: &str,
        generation: Generation,
        seq_hint: Seq,
    ) -> EventEnvelope {
        EventEnvelope {
            event_id: format!(
                "evt_{}_{}_{}",
                session_id,
                seq_hint,
                event_type.replace('.', "_")
            ),
            session_id: Some(session_id.into()),
            seq: seq_hint,
            generation,
            event_type: event_type.into(),
            source: Actor::new(ActorKind::System),
            payload: json!({"ts": seq_hint}),
            occurred_at: 1719000000000 + seq_hint as i64,
            causation_id: None,
            correlation_id: None,
        }
    }

    fn append_n(log: &EventLog, session_id: &str, n: u64, generation: Generation) {
        for i in 1..=n {
            let event = make_event(session_id, "test.event", generation, i);
            log.append(event).unwrap();
        }
    }

    fn insert_session(db: &DbOwner, session_id: &str, status: &str) {
        db.conn()
            .execute(
                "INSERT OR IGNORE INTO sessions (id, created_at, workspace, status) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![session_id, 1000.0, "/tmp", status],
            )
            .unwrap();
    }

    fn create_service_with_session(session_id: &str, status: &str) -> ProjectionService {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        insert_session(&db, session_id, status);
        let log = EventLog::new(db);
        ProjectionService::new(log)
    }

    #[test]
    fn test_snapshot_contains_headless_projection_without_tool_payloads() {
        let service = create_service_with_session("sess-projection", "active");
        let conn = service.event_log().db.conn();
        conn.execute(
            "INSERT INTO tasks \
             (id, session_id, subject, description, status, agent_type, assigned_agent, created_at, updated_at) \
             VALUES ('task-1', 'sess-projection', 'Subject', 'Description', 'created', 'general', 'agent-1', 1.0, 2.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agent_state \
             (session_id, agent_id, agent_name, agent_role, task_id, status, stopped, iteration, timestamp) \
             VALUES ('sess-projection', 'agent-1', 'Agent', 'worker', 'task-1', 'running', 0, 3, 4.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tool_calls \
             (id, session_id, tool_name, tool_type, status, args_json, result_json, started_at, completed_at) \
             VALUES ('tc-1', 'sess-projection', 'file_read', 'native', 'completed', '{\"secret\":\"omit\"}', '{\"body\":\"omit\"}', 5, 6)",
            [],
        )
        .unwrap();
        drop(conn);

        let snapshot = service.snapshot("sess-projection").unwrap();
        let projection = &snapshot.payload["headless_projection"];
        assert_eq!(projection["boundary"], "rust-core-headless");
        assert_eq!(projection["collection_limit"], SNAPSHOT_COLLECTION_LIMIT);
        assert_eq!(projection["tasks"][0]["task_id"], "task-1");
        assert_eq!(projection["agents"][0]["agent_id"], "agent-1");
        assert_eq!(projection["tool_calls"][0]["tool_call_id"], "tc-1");
        assert!(projection["tool_calls"][0].get("args_json").is_none());
        assert!(projection["tool_calls"][0].get("result_json").is_none());
        assert_eq!(projection["redaction"]["tool_args"], "omitted");
    }

    // -----------------------------------------------------------------------
    // GS-022: Cursor reconnect delta
    // -----------------------------------------------------------------------

    #[test]
    fn test_gs022_reconnect_delta() {
        let service = create_service_with_session("sess-gs022", "active");
        append_n(service.event_log(), "sess-gs022", 8, 1);

        let cursor = ReplayCursor {
            session_id: "sess-gs022".into(),
            last_known_seq: 5,
            generation: None,
        };

        let result = service.connect(&cursor).unwrap();
        match result {
            ConnectResult::Delta { events, latest_seq } => {
                assert_eq!(events.len(), 3);
                assert_eq!(events[0].seq, 6);
                assert_eq!(events[1].seq, 7);
                assert_eq!(events[2].seq, 8);
                assert_eq!(latest_seq, 8);
            }
            other => panic!("Expected Delta, got {other:?}"),
        }
    }

    #[test]
    fn test_gs022_reconnect_no_delta_when_caught_up() {
        let service = create_service_with_session("sess-gs022b", "active");
        append_n(service.event_log(), "sess-gs022b", 8, 1);

        let cursor = ReplayCursor {
            session_id: "sess-gs022b".into(),
            last_known_seq: 8,
            generation: None,
        };

        let result = service.connect(&cursor).unwrap();
        match result {
            ConnectResult::Delta { events, latest_seq } => {
                assert!(events.is_empty());
                assert_eq!(latest_seq, 8);
            }
            other => panic!("Expected Delta, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // GS-023: Gap too large triggers snapshot
    // -----------------------------------------------------------------------

    #[test]
    fn test_gs023_gap_too_large_returns_snapshot_required() {
        let service = create_service_with_session("sess-gs023", "active");
        append_n(service.event_log(), "sess-gs023", 200, 1);

        let cursor = ReplayCursor {
            session_id: "sess-gs023".into(),
            last_known_seq: 50,
            generation: None,
        };

        let result = service.connect(&cursor).unwrap();
        match result {
            ConnectResult::SnapshotRequired {
                message,
                latest_seq,
            } => {
                assert!(message.contains("Gap too large"));
                assert_eq!(latest_seq, 200);
            }
            other => panic!("Expected SnapshotRequired, got {other:?}"),
        }
    }

    #[test]
    fn test_gs023_snapshot_contains_required_fields() {
        let service = create_service_with_session("sess-gs023b", "active");
        append_n(service.event_log(), "sess-gs023b", 200, 1);

        let snap = service.snapshot("sess-gs023b").unwrap();

        assert_eq!(snap.session_id, "sess-gs023b");
        assert_eq!(snap.last_seq, 200);
        assert_eq!(snap.generation, 1);
        assert_eq!(snap.status, "active");
        assert_eq!(snap.payload["snapshot_seq"], 200);
        assert_eq!(snap.payload["generation"], 1);
    }

    // -----------------------------------------------------------------------
    // Cursor ahead of server
    // -----------------------------------------------------------------------

    #[test]
    fn test_cursor_ahead_of_server_returns_snapshot_required() {
        let service = create_service_with_session("sess-ahead", "active");
        append_n(service.event_log(), "sess-ahead", 10, 1);

        let cursor = ReplayCursor {
            session_id: "sess-ahead".into(),
            last_known_seq: 99,
            generation: None,
        };

        let result = service.connect(&cursor).unwrap();
        match result {
            ConnectResult::SnapshotRequired {
                message,
                latest_seq,
            } => {
                assert!(
                    message.contains("ahead of server"),
                    "Message should mention client is ahead: {message}"
                );
                assert_eq!(latest_seq, 10);
            }
            other => panic!("Expected SnapshotRequired, got {other:?}"),
        }
    }

    #[test]
    fn test_cursor_before_compacted_seq_returns_snapshot_required() {
        let service = create_service_with_session("sess-compacted", "active");
        append_n(service.event_log(), "sess-compacted", 10, 1);
        service.event_log().compact("sess-compacted", 5).unwrap();

        let cursor = ReplayCursor {
            session_id: "sess-compacted".into(),
            last_known_seq: 4,
            generation: None,
        };

        let result = service.connect(&cursor).unwrap();
        match result {
            ConnectResult::SnapshotRequired {
                message,
                latest_seq,
            } => {
                assert!(message.contains("compacted seq"));
                assert_eq!(latest_seq, 10);
            }
            other => panic!("Expected SnapshotRequired, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_snapshot_session_not_found_returns_unknown_status() {
        let service = create_service_with_session("sess-exists", "active");
        // Do NOT insert "sess-ghost" into sessions table
        append_n(service.event_log(), "sess-ghost", 5, 1);

        let snap = service.snapshot("sess-ghost").unwrap();
        assert_eq!(snap.session_id, "sess-ghost");
        assert_eq!(snap.last_seq, 5);
        assert_eq!(snap.generation, 1);
        // No sessions row → status falls back to "unknown"
        assert_eq!(snap.status, "unknown");
    }

    #[test]
    fn test_snapshot_status_reads_from_sessions_table() {
        let service = create_service_with_session("sess-status", "completed");
        append_n(service.event_log(), "sess-status", 3, 1);

        let snap = service.snapshot("sess-status").unwrap();
        assert_eq!(snap.status, "completed");
        assert_eq!(snap.last_seq, 3);
    }

    // -----------------------------------------------------------------------
    // Event log replay ordering preserved
    // -----------------------------------------------------------------------

    #[test]
    fn test_event_log_replay_ordering_preserved() {
        let service = create_service_with_session("sess-order", "active");
        let log = service.event_log();
        append_n(log, "sess-order", 5, 1);

        let batch = log.replay("sess-order", 0, 100).unwrap();
        for i in 0..batch.events.len().saturating_sub(1) {
            assert!(
                batch.events[i].seq < batch.events[i + 1].seq,
                "Events must be in ascending seq order"
            );
        }
    }
}
