use serde::{Deserialize, Serialize};

// === Primitive type aliases ===

/// Unix timestamp (milliseconds since epoch).
pub type Timestamp = i64;

pub type SessionId = String;
pub type RequestId = String;
pub type EventId = String;
pub type SnapshotId = String;
pub type IdempotencyKey = String;
pub type Seq = u64;
pub type Generation = u64;

// === Cursor / ReplayCursor ===

/// Cursor for event replay — sent by clients on reconnect to request delta events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCursor {
    pub session_id: SessionId,
    pub last_known_seq: Seq,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<Generation>,
}
