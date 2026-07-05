use crate::actor::Actor;
use crate::types::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A durable canonical event emitted by Rust Core after a state transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: EventId,
    pub session_id: Option<SessionId>,
    pub seq: Seq,
    pub generation: Generation,
    pub event_type: String,
    pub source: Actor,
    pub payload: Value,
    pub occurred_at: Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<EventId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<RequestId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorKind;
    use serde_json::json;

    fn make_event() -> EventEnvelope {
        EventEnvelope {
            event_id: "evt_001_1_session.created".into(),
            session_id: Some("sess-001".into()),
            seq: 1,
            generation: 1,
            event_type: "session.created".into(),
            source: Actor::new(ActorKind::System),
            payload: json!({ "session_id": "sess-001", "workspace": "/tmp/test" }),
            occurred_at: 1719000000000,
            causation_id: None,
            correlation_id: None,
        }
    }

    #[test]
    fn test_event_round_trip_required() {
        let evt = make_event();
        let json = serde_json::to_string(&evt).unwrap();
        let deserialized: EventEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.event_id, evt.event_id);
        assert_eq!(deserialized.seq, 1);
        assert_eq!(deserialized.generation, 1);
        assert_eq!(deserialized.event_type, "session.created");
        assert_eq!(deserialized.source.kind, ActorKind::System);
    }

    #[test]
    fn test_event_with_causation_and_correlation() {
        let mut evt = make_event();
        evt.causation_id = Some("evt_000_0_req-001".into());
        evt.correlation_id = Some("req-001".into());

        let json = serde_json::to_string(&evt).unwrap();
        let deserialized: EventEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.causation_id.unwrap(), "evt_000_0_req-001");
        assert_eq!(deserialized.correlation_id.unwrap(), "req-001");
    }

    #[test]
    fn test_event_optionals_skipped_when_none() {
        let evt = make_event();
        let json = serde_json::to_string(&evt).unwrap();
        assert!(
            !json.contains("causation_id"),
            "None causation_id should be skipped"
        );
        assert!(
            !json.contains("correlation_id"),
            "None correlation_id should be skipped"
        );
    }

    #[test]
    fn test_event_payload_is_json_value() {
        let evt = make_event();
        let json = serde_json::to_string(&evt).unwrap();
        assert!(json.contains(r#""session_id":"sess-001""#));
    }
}
