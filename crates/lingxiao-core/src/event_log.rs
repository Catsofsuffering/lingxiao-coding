use crate::persistence::DbOwner;
use lingxiao_core_protocol::actor::{Actor, ActorKind};
use lingxiao_core_protocol::event::EventEnvelope;
use lingxiao_core_protocol::types::*;
use rusqlite::{params, OptionalExtension, Result, Transaction};

pub const MAX_REPLAY_EVENTS: u64 = 100;

#[derive(Debug, Clone)]
pub struct Cursor {
    pub session_id: String,
    pub last_known_seq: u64,
}

impl Cursor {
    pub fn new(session_id: impl Into<String>, last_known_seq: u64) -> Self {
        Self {
            session_id: session_id.into(),
            last_known_seq,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReplayResult {
    Events(Vec<EventEnvelope>),
    SnapshotRequired { message: String },
}

pub fn check_gap(cursor_seq: u64, latest_seq: u64) -> ReplayResult {
    let gap = latest_seq.saturating_sub(cursor_seq);
    if gap > MAX_REPLAY_EVENTS {
        ReplayResult::SnapshotRequired {
            message: format!("Gap too large ({gap} > {MAX_REPLAY_EVENTS}). Request full snapshot."),
        }
    } else {
        ReplayResult::Events(Vec::new())
    }
}

/// Outcome of an append-with-generation-check call.
#[derive(Debug, Clone)]
pub enum AppendOutcome {
    /// Event was written to the log and is accepted for state transition.
    Accepted(EventEnvelope),
    /// Event was written to the log (audit trail) but REJECTED for state transition
    /// because its generation is older than the session's current generation.
    Rejected(EventEnvelope),
    /// Event was already present (duplicate event_id). No new row inserted.
    Duplicate(EventEnvelope),
}

/// A page of replayed events.
#[derive(Debug, Clone)]
pub struct ReplayBatch {
    pub events: Vec<EventEnvelope>,
    pub has_more: bool,
}

/// Durable ordered event log backed by SQLite.
/// Wraps a [`DbOwner`] and provides append / replay / generation-check operations.
#[derive(Clone)]
pub struct EventLog {
    pub(crate) db: DbOwner,
}

impl EventLog {
    pub fn new(db: DbOwner) -> Self {
        Self { db }
    }

    /// Append an event unconditionally.
    ///
    /// Allocates the next per-session `seq`, inserts the row, and updates
    /// `event_log_meta.last_seq`. Returns the fully materialized `EventEnvelope`.
    ///
    /// If an event with the same `event_id` already exists, the existing
    /// event is returned and no new row is inserted (idempotent).
    pub fn append(&self, mut event: EventEnvelope) -> Result<EventEnvelope> {
        self.db.with_transaction(|tx| {
            if let Some(existing) = try_fetch_event_by_id(tx, &event.event_id)? {
                return Ok(existing);
            }
            ensure_meta(tx, &event.session_id)?;
            let next_seq = allocate_seq(tx, &event.session_id)?;
            event.seq = next_seq;
            insert_event(tx, &event)?;
            update_meta_seq(tx, &event.session_id, next_seq)?;
            Ok(event)
        })
    }

    /// Append an event with generation-gate check.
    ///
    /// * Always writes the event to the log (audit trail).
    /// * If `event.generation >= current_generation` → `Accepted` (and bumps
    ///   `current_generation` if the event carries a newer one).
    /// * If `event.generation < current_generation` → `Rejected` (old-gen event).
    /// * If `event_id` is a duplicate → `Duplicate` (no write).
    pub fn append_with_generation_check(&self, mut event: EventEnvelope) -> Result<AppendOutcome> {
        self.db.with_transaction(|tx| {
            if let Some(existing) = try_fetch_event_by_id(tx, &event.event_id)? {
                return Ok(AppendOutcome::Duplicate(existing));
            }
            ensure_meta(tx, &event.session_id)?;
            let current_gen = get_current_generation(tx, &event.session_id)?;

            let next_seq = allocate_seq(tx, &event.session_id)?;
            event.seq = next_seq;
            insert_event(tx, &event)?;
            update_meta_seq(tx, &event.session_id, next_seq)?;

            if event.generation >= current_gen {
                if event.generation > current_gen {
                    update_generation(tx, &event.session_id, event.generation)?;
                }
                Ok(AppendOutcome::Accepted(event))
            } else {
                Ok(AppendOutcome::Rejected(event))
            }
        })
    }

    /// Replay events for `session_id` with `seq > from_seq`, up to `limit` rows.
    ///
    /// Returns [`ReplayBatch`] with `has_more` set when more rows exist
    /// beyond the returned page.
    pub fn replay(&self, session_id: &str, from_seq: Seq, limit: u64) -> Result<ReplayBatch> {
        let conn = self.db.conn();
        let mut stmt = conn.prepare(
            "SELECT event_id, session_id, seq, generation, event_type, \
                    source_kind, source_id, payload, occurred_at, \
                    causation_id, correlation_id \
             FROM event_log \
             WHERE session_id = ?1 AND seq > ?2 \
             ORDER BY seq \
             LIMIT ?3",
        )?;

        let rows = stmt.query_map(params![session_id, from_seq, limit + 1], row_to_envelope)?;

        let mut events: Vec<EventEnvelope> = Vec::new();
        for row in rows {
            events.push(row?);
        }

        let has_more = events.len() as u64 > limit;
        if has_more {
            events.truncate(limit as usize);
        }

        Ok(ReplayBatch { events, has_more })
    }

    pub fn compact(&self, session_id: &str, compact_through_seq: Seq) -> Result<u64> {
        self.db.with_transaction(|tx| {
            ensure_meta(tx, &Some(session_id.to_string()))?;
            let latest_seq: Seq = tx.query_row(
                "SELECT last_seq FROM event_log_meta WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )?;
            let bounded_seq = compact_through_seq.min(latest_seq);
            let deleted = tx.execute(
                "DELETE FROM event_log WHERE session_id = ?1 AND seq <= ?2",
                params![session_id, bounded_seq],
            )?;
            tx.execute(
                "UPDATE event_log_meta SET compacted_seq = MAX(compacted_seq, ?1) WHERE session_id = ?2",
                params![bounded_seq, session_id],
            )?;
            Ok(deleted as u64)
        })
    }

    pub fn compacted_seq(&self, session_id: &str) -> Result<Seq> {
        let conn = self.db.conn();
        let seq = conn
            .query_row(
                "SELECT compacted_seq FROM event_log_meta WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(seq)
    }

    /// Return the latest allocated seq for a session (or 0 if none).
    pub fn latest_seq(&self, session_id: &str) -> Result<Seq> {
        let conn = self.db.conn();
        let seq = conn
            .query_row(
                "SELECT last_seq FROM event_log_meta WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(seq)
    }

    /// Return the current generation for a session (or 1 if none).
    pub fn current_generation(&self, session_id: &str) -> Result<Generation> {
        let conn = self.db.conn();
        let generation = conn
            .query_row(
                "SELECT current_generation FROM event_log_meta WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(1);
        Ok(generation)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Convert a snake_case string (e.g. "system") to an ActorKind.
fn actor_kind_from_str(s: &str) -> std::result::Result<ActorKind, String> {
    let json = format!("\"{}\"", s);
    serde_json::from_str(&json).map_err(|e| e.to_string())
}

/// Serialise an ActorKind to its snake_case JSON string representation,
/// then strip the surrounding quotes.
fn actor_kind_to_str(kind: &ActorKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| format!("{:?}", kind).to_lowercase())
}

/// Helper: the session_id column is NOT NULL; use empty string for None.
pub(crate) fn sid(session_id: &Option<String>) -> &str {
    session_id.as_deref().unwrap_or("")
}

pub(crate) fn try_fetch_event_by_id(
    tx: &Transaction,
    event_id: &str,
) -> Result<Option<EventEnvelope>> {
    let mut stmt = tx.prepare(
        "SELECT event_id, session_id, seq, generation, event_type, \
                source_kind, source_id, payload, occurred_at, \
                causation_id, correlation_id \
         FROM event_log WHERE event_id = ?1",
    )?;
    let mut rows = stmt.query_map(params![event_id], row_to_envelope)?;
    match rows.next() {
        Some(Ok(event)) => Ok(Some(event)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

pub(crate) fn ensure_meta(tx: &Transaction, session_id: &Option<String>) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO event_log_meta (session_id, last_seq, current_generation) \
         VALUES (?1, 0, 1)",
        params![sid(session_id)],
    )?;
    Ok(())
}

pub(crate) fn allocate_seq(tx: &Transaction, session_id: &Option<String>) -> Result<Seq> {
    let seq: Seq = tx.query_row(
        "SELECT last_seq FROM event_log_meta WHERE session_id = ?1",
        params![sid(session_id)],
        |row| row.get(0),
    )?;
    Ok(seq + 1)
}

pub(crate) fn update_meta_seq(
    tx: &Transaction,
    session_id: &Option<String>,
    seq: Seq,
) -> Result<()> {
    tx.execute(
        "UPDATE event_log_meta SET last_seq = ?1 WHERE session_id = ?2",
        params![seq, sid(session_id)],
    )?;
    Ok(())
}

pub(crate) fn get_current_generation(
    tx: &Transaction,
    session_id: &Option<String>,
) -> Result<Generation> {
    let gen: Generation = tx.query_row(
        "SELECT current_generation FROM event_log_meta WHERE session_id = ?1",
        params![sid(session_id)],
        |row| row.get(0),
    )?;
    Ok(gen)
}

pub(crate) fn update_generation(
    tx: &Transaction,
    session_id: &Option<String>,
    generation: Generation,
) -> Result<()> {
    tx.execute(
        "UPDATE event_log_meta SET current_generation = ?1 WHERE session_id = ?2",
        params![generation, sid(session_id)],
    )?;
    Ok(())
}

pub(crate) fn insert_event(tx: &Transaction, event: &EventEnvelope) -> Result<()> {
    let source_kind = actor_kind_to_str(&event.source.kind);
    let payload = redact_event_payload(&event.payload).to_string();

    tx.execute(
        "INSERT INTO event_log \
         (event_id, session_id, seq, generation, event_type, \
          source_kind, source_id, payload, occurred_at, \
          causation_id, correlation_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            event.event_id,
            sid(&event.session_id),
            event.seq,
            event.generation,
            event.event_type,
            source_kind,
            event.source.id,
            payload,
            event.occurred_at,
            event.causation_id,
            event.correlation_id,
        ],
    )?;
    Ok(())
}

fn redact_event_payload(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(redact_secret_text(text)),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(redact_event_payload).collect())
        }
        serde_json::Value::Object(object) => serde_json::Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), redact_event_payload(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn redact_secret_text(text: &str) -> String {
    let mut redacted = redact_prefixed_secret(text, "sk-");
    redacted = redact_prefixed_secret(&redacted, "sk-ant-");
    redacted = redact_prefixed_secret(&redacted, "AKIA");
    redacted = redact_prefixed_secret(&redacted, "ASIA");
    redact_bearer_token(&redacted)
}

fn redact_bearer_token(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(relative_start) = lower[cursor..].find("bearer") {
        let start = cursor + relative_start;
        let boundary_before = start == 0 || !text.as_bytes()[start - 1].is_ascii_alphanumeric();
        if !boundary_before {
            output.push_str(&text[cursor..start + 6]);
            cursor = start + 6;
            continue;
        }
        output.push_str(&text[cursor..start]);
        output.push_str("Bearer");
        let after = start + 6;
        let mut token_start = after;
        while token_start < text.len() && text.as_bytes()[token_start] == b' ' {
            token_start += 1;
        }
        if token_start > after {
            output.push_str(&text[after..token_start]);
        }
        let token_end = text[token_start..]
            .find(|ch: char| {
                ch.is_whitespace()
                    || matches!(ch, '"' | '\'' | ',' | ';' | ')' | ']' | '}' | '<' | '>')
            })
            .map(|relative_end| token_start + relative_end)
            .unwrap_or(text.len());
        if token_end > token_start {
            output.push_str("[redacted]");
            cursor = token_end;
        } else {
            cursor = token_start;
        }
    }
    output.push_str(&text[cursor..]);
    output
}

fn redact_prefixed_secret(text: &str, prefix: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(relative_start) = text[cursor..].find(prefix) {
        let start = cursor + relative_start;
        output.push_str(&text[cursor..start]);
        let end = text[start..]
            .find(|ch: char| {
                ch.is_whitespace()
                    || matches!(ch, '"' | '\'' | ',' | ';' | ')' | ']' | '}' | '<' | '>')
            })
            .map(|relative_end| start + relative_end)
            .unwrap_or(text.len());
        output.push_str("[redacted]");
        cursor = end;
    }
    output.push_str(&text[cursor..]);
    output
}

fn row_to_envelope(row: &rusqlite::Row) -> rusqlite::Result<EventEnvelope> {
    let payload_str: String = row.get(7)?;
    let payload: serde_json::Value = serde_json::from_str(&payload_str).map_err(to_sqlite_error)?;

    let source_kind_str: String = row.get(5)?;
    let source_kind = actor_kind_from_str(&source_kind_str)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;

    Ok(EventEnvelope {
        event_id: row.get(0)?,
        session_id: match row.get::<_, String>(1)? {
            s if s.is_empty() => None,
            s => Some(s),
        },
        seq: row.get(2)?,
        generation: row.get(3)?,
        event_type: row.get(4)?,
        source: Actor {
            kind: source_kind,
            id: row.get(6)?,
        },
        payload,
        occurred_at: row.get(8)?,
        causation_id: row.get(9)?,
        correlation_id: row.get(10)?,
    })
}

fn to_sqlite_error(e: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(e.into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::DbOwner;
    use serde_json::json;

    fn setup() -> EventLog {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        EventLog::new(db)
    }

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

    // -----------------------------------------------------------------------
    // GS-020: Ordered event replay
    // -----------------------------------------------------------------------

    #[test]
    fn test_gs020_replay_full_range() {
        let log = setup();
        append_n(&log, "sess-gs020a", 10, 1);

        let batch = log.replay("sess-gs020a", 5, 100).unwrap();

        assert_eq!(batch.events.len(), 5); // seq=6,7,8,9,10
        assert!(!batch.has_more);
        assert_eq!(batch.events[0].seq, 6);
        assert_eq!(batch.events[1].seq, 7);
        assert_eq!(batch.events[2].seq, 8);
        assert_eq!(batch.events[3].seq, 9);
        assert_eq!(batch.events[4].seq, 10);
    }

    #[test]
    fn test_gs020_replay_paginated() {
        let log = setup();
        append_n(&log, "sess-gs020b", 10, 1);

        let batch = log.replay("sess-gs020b", 5, 2).unwrap();

        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.events[0].seq, 6);
        assert_eq!(batch.events[1].seq, 7);
        assert!(batch.has_more);
    }

    #[test]
    fn test_gs020_replay_empty_range() {
        let log = setup();
        append_n(&log, "sess-gs020c", 10, 1);

        let batch = log.replay("sess-gs020c", 10, 100).unwrap();

        assert!(batch.events.is_empty());
        assert!(!batch.has_more);
    }

    #[test]
    fn test_gs020_replay_cross_session_independence() {
        let log = setup();
        append_n(&log, "sess-a", 5, 1);
        append_n(&log, "sess-b", 3, 1);

        let batch_a = log.replay("sess-a", 2, 100).unwrap();
        assert_eq!(batch_a.events.len(), 3); // seq=3,4,5
        assert_eq!(batch_a.events[0].seq, 3);

        let batch_b = log.replay("sess-b", 1, 100).unwrap();
        assert_eq!(batch_b.events.len(), 2); // seq=2,3
        assert_eq!(batch_b.events[0].seq, 2);
    }

    // -----------------------------------------------------------------------
    // GS-021: Late generation detection and rejection
    // -----------------------------------------------------------------------

    #[test]
    fn test_gs021_late_generation_rejected() {
        let log = setup();
        // First append a generation-1 event (this sets current_generation = 1)
        let ev1 = make_event("sess-gs021", "test.first", 1, 1);
        log.append(ev1).unwrap();

        // Bump current_generation to 2 by appending a generation-2 event
        // using append_with_generation_check
        let ev2 = make_event("sess-gs021", "test.bump", 2, 2);
        let outcome = log.append_with_generation_check(ev2).unwrap();
        assert!(matches!(outcome, AppendOutcome::Accepted(_)));

        assert_eq!(log.current_generation("sess-gs021").unwrap(), 2);

        // Now a late generation=1 event arrives
        let late = make_event("sess-gs021", "test.late", 1, 99);
        let outcome = log.append_with_generation_check(late).unwrap();

        match outcome {
            AppendOutcome::Rejected(event) => {
                assert_eq!(event.generation, 1);
                assert_eq!(event.seq, 3);
            }
            other => panic!("Expected Rejected, got {:?}", other),
        }

        // current_generation must NOT have been downgraded
        assert_eq!(log.current_generation("sess-gs021").unwrap(), 2);
    }

    #[test]
    fn test_gs021_same_generation_accepted() {
        let log = setup();
        let ev1 = make_event("sess-gs021b", "test.first", 1, 1);
        log.append(ev1).unwrap();

        // generation=1 event should be accepted (current_gen = 1)
        let ev2 = make_event("sess-gs021b", "test.second", 1, 2);
        let outcome = log.append_with_generation_check(ev2).unwrap();
        assert!(matches!(outcome, AppendOutcome::Accepted(_)));
    }

    #[test]
    fn test_gs021_newer_generation_bumps_and_accepted() {
        let log = setup();
        let ev1 = make_event("sess-gs021c", "test.first", 1, 1);
        log.append(ev1).unwrap();

        // generation=3 is newer than current=1 → accepted AND generation is bumped
        let ev2 = make_event("sess-gs021c", "test.newgen", 3, 2);
        let outcome = log.append_with_generation_check(ev2).unwrap();
        assert!(matches!(outcome, AppendOutcome::Accepted(_)));
        assert_eq!(log.current_generation("sess-gs021c").unwrap(), 3);
    }

    // -----------------------------------------------------------------------
    // Duplicate event_id
    // -----------------------------------------------------------------------

    #[test]
    fn test_duplicate_event_id_append() {
        let log = setup();
        let ev = make_event("sess-dup", "test.event", 1, 1);
        let first = log.append(ev).unwrap();
        assert_eq!(first.seq, 1);

        let dup = make_event("sess-dup", "test.event", 1, 1);
        let second = log.append(dup).unwrap();
        // Must return the existing event (seq=1) rather than creating a new row
        assert_eq!(second.seq, 1);

        // Verify only one row in the log
        let batch = log.replay("sess-dup", 0, 100).unwrap();
        assert_eq!(batch.events.len(), 1);
    }

    #[test]
    fn test_duplicate_event_id_generation_check() {
        let log = setup();
        let ev = make_event("sess-dup-gen", "test.event", 1, 1);
        let outcome = log.append_with_generation_check(ev).unwrap();
        assert!(matches!(outcome, AppendOutcome::Accepted(_)));

        let dup = make_event("sess-dup-gen", "test.event", 1, 1);
        let outcome = log.append_with_generation_check(dup).unwrap();
        assert!(matches!(outcome, AppendOutcome::Duplicate(_)));

        // Still only one row in the log
        let batch = log.replay("sess-dup-gen", 0, 100).unwrap();
        assert_eq!(batch.events.len(), 1);
    }

    #[test]
    fn test_event_id_uniqueness_enforced_by_db() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let conn = db.conn();
        conn.execute(
            "INSERT INTO event_log \
             (event_id, session_id, seq, generation, event_type, source_kind, \
              payload, occurred_at) \
             VALUES ('dup-test', 's', 1, 1, 't', 'system', '{}', 0)",
            [],
        )
        .unwrap();
        let err = conn.execute(
            "INSERT INTO event_log \
             (event_id, session_id, seq, generation, event_type, source_kind, \
              payload, occurred_at) \
             VALUES ('dup-test', 's', 2, 1, 't', 'system', '{}', 0)",
            [],
        );
        assert!(
            err.is_err(),
            "DB UNIQUE constraint on event_id should reject duplicate"
        );
    }

    // -----------------------------------------------------------------------
    // Schema constraints
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_seq_uniqueness_enforced() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let conn = db.conn();
        conn.execute(
            "INSERT INTO event_log \
             (event_id, session_id, seq, generation, event_type, source_kind, \
              payload, occurred_at) \
             VALUES ('a', 's1', 1, 1, 't', 'system', '{}', 0)",
            [],
        )
        .unwrap();
        let err = conn.execute(
            "INSERT INTO event_log \
             (event_id, session_id, seq, generation, event_type, source_kind, \
              payload, occurred_at) \
             VALUES ('b', 's1', 1, 1, 't', 'system', '{}', 0)",
            [],
        );
        assert!(
            err.is_err(),
            "PRIMARY KEY (session_id, seq) should reject duplicate"
        );
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_append_and_latest_seq() {
        let log = setup();
        assert_eq!(log.latest_seq("no-such-session").unwrap(), 0);

        let ev = make_event("sess-edge", "test.first", 1, 1);
        log.append(ev).unwrap();
        assert_eq!(log.latest_seq("sess-edge").unwrap(), 1);

        let ev2 = make_event("sess-edge", "test.second", 1, 2);
        log.append(ev2).unwrap();
        assert_eq!(log.latest_seq("sess-edge").unwrap(), 2);
    }

    #[test]
    fn test_seq_assignment_starts_at_1() {
        let log = setup();
        let ev = make_event("sess-seq1", "test.event", 1, 999);
        let appended = log.append(ev).unwrap();
        assert_eq!(appended.seq, 1);
    }

    #[test]
    fn test_seq_monotonic_per_session() {
        let log = setup();
        for i in 0..5 {
            let ev = make_event("sess-mon", "test.event", 1, i);
            let appended = log.append(ev).unwrap();
            assert_eq!(appended.seq, i + 1, "seq must be 1-based monotonic");
        }
    }

    #[test]
    fn test_session_id_none_uses_empty_string() {
        let log = setup();
        let ev = EventEnvelope {
            event_id: "evt_none_session".into(),
            session_id: None,
            seq: 0,
            generation: 1,
            event_type: "daemon.event".into(),
            source: Actor::new(ActorKind::System),
            payload: json!({"key": "val"}),
            occurred_at: 1719000000000,
            causation_id: None,
            correlation_id: None,
        };
        let appended = log.append(ev).unwrap();
        assert_eq!(appended.seq, 1);
        // Replay with empty string should work
        let batch = log.replay("", 0, 100).unwrap();
        assert_eq!(batch.events.len(), 1);
    }

    #[test]
    fn test_source_kind_stored_as_snake_case() {
        let log = setup();
        let ev = EventEnvelope {
            event_id: "evt_source_kind".into(),
            session_id: Some("sess-sk".into()),
            seq: 0,
            generation: 1,
            event_type: "test.source_kind".into(),
            source: Actor::new(ActorKind::User),
            payload: json!({}),
            occurred_at: 1000,
            causation_id: None,
            correlation_id: None,
        };
        log.append(ev).unwrap();

        let conn = log.db.conn();
        let sk: String = conn
            .query_row(
                "SELECT source_kind FROM event_log WHERE event_id = 'evt_source_kind'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sk, "user");
    }

    #[test]
    fn test_source_id_roundtrip() {
        let log = setup();
        let ev = EventEnvelope {
            event_id: "evt_source_id".into(),
            session_id: Some("sess-sid".into()),
            seq: 0,
            generation: 1,
            event_type: "test.source_id".into(),
            source: Actor::with_id(ActorKind::Agent, "agent-001"),
            payload: json!({}),
            occurred_at: 1000,
            causation_id: None,
            correlation_id: None,
        };
        log.append(ev).unwrap();

        let batch = log.replay("sess-sid", 0, 10).unwrap();
        assert_eq!(batch.events[0].source.id.as_deref(), Some("agent-001"));
    }

    #[test]
    fn test_causation_correlation_roundtrip() {
        let log = setup();
        let mut ev = make_event("sess-cc", "test.cc", 1, 1);
        ev.causation_id = Some("evt_parent".into());
        ev.correlation_id = Some("req-001".into());
        log.append(ev).unwrap();

        let batch = log.replay("sess-cc", 0, 10).unwrap();
        assert_eq!(batch.events[0].causation_id.as_deref(), Some("evt_parent"));
        assert_eq!(batch.events[0].correlation_id.as_deref(), Some("req-001"));
    }

    #[test]
    fn test_check_gap_small() {
        let result = check_gap(50, 60);
        assert!(matches!(result, ReplayResult::Events(_)));
    }

    #[test]
    fn test_check_gap_large() {
        let result = check_gap(50, 200);
        assert!(matches!(result, ReplayResult::SnapshotRequired { .. }));
    }

    #[test]
    fn test_check_gap_zero() {
        let result = check_gap(100, 100);
        assert!(matches!(result, ReplayResult::Events(_)));
    }
}
