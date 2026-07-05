use crate::types::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A stable projection of canonical session state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEnvelope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<SnapshotId>,
    pub session_id: SessionId,
    pub generation: Generation,
    pub last_seq: Seq,
    pub status: String,
    pub payload: Value,
    pub created_at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_snapshot_round_trip() {
        let snap = SnapshotEnvelope {
            snapshot_id: Some("snap-001".into()),
            session_id: "sess-001".into(),
            generation: 2,
            last_seq: 42,
            status: "active".into(),
            payload: json!({ "tasks": [], "conversation": [] }),
            created_at: 1719000000000,
        };

        let json = serde_json::to_string(&snap).unwrap();
        let deserialized: SnapshotEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.snapshot_id.unwrap(), "snap-001");
        assert_eq!(deserialized.session_id, "sess-001");
        assert_eq!(deserialized.generation, 2);
        assert_eq!(deserialized.last_seq, 42);
        assert_eq!(deserialized.status, "active");
        assert_eq!(deserialized.created_at, 1719000000000);
    }

    #[test]
    fn test_snapshot_without_id() {
        let snap = SnapshotEnvelope {
            snapshot_id: None,
            session_id: "sess-002".into(),
            generation: 1,
            last_seq: 10,
            status: "completed".into(),
            payload: json!({}),
            created_at: 1719000000001,
        };

        let json = serde_json::to_string(&snap).unwrap();
        assert!(
            !json.contains("snapshot_id"),
            "None snapshot_id should be skipped"
        );

        let deserialized: SnapshotEnvelope = serde_json::from_str(&json).unwrap();
        assert!(deserialized.snapshot_id.is_none());
        assert_eq!(deserialized.last_seq, 10);
    }
}
