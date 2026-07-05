use crate::actor::Actor;
use crate::error::CoreError;
use crate::event::EventEnvelope;
use crate::types::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A command envelope sent from a client/adapter/sidecar to Rust Core.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub request_id: RequestId,
    pub method: String,
    pub params: Value,
    pub actor: Actor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<IdempotencyKey>,
    pub submitted_at: Timestamp,
}

/// Response from a command dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub request_id: RequestId,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<CoreError>,
    pub events: Vec<EventEnvelope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_seq: Option<Seq>,
}

impl CommandResponse {
    pub fn ok(request_id: RequestId, result: Option<Value>, latest_seq: Option<Seq>) -> Self {
        Self {
            request_id,
            success: true,
            result,
            error: None,
            events: Vec::new(),
            latest_seq,
        }
    }

    pub fn with_event(request_id: RequestId, event: EventEnvelope, result: Option<Value>) -> Self {
        let latest_seq = Some(event.seq);
        Self {
            request_id,
            success: true,
            result,
            error: None,
            events: vec![event],
            latest_seq,
        }
    }

    pub fn err(request_id: RequestId, error: CoreError) -> Self {
        Self {
            request_id,
            success: false,
            result: None,
            error: Some(error),
            events: Vec::new(),
            latest_seq: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorKind;
    use serde_json::json;

    #[test]
    fn test_command_envelope_round_trip() {
        let cmd = CommandEnvelope {
            request_id: "req-001".into(),
            method: "session.create".into(),
            params: json!({ "workspace": "/tmp/test" }),
            actor: Actor::new(ActorKind::User),
            session_id: None,
            idempotency_key: Some("idem-001".into()),
            submitted_at: 1719000000000,
        };

        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: CommandEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.request_id, cmd.request_id);
        assert_eq!(deserialized.method, cmd.method);
        assert_eq!(deserialized.actor.kind, ActorKind::User);
        assert_eq!(deserialized.idempotency_key, Some("idem-001".into()));
        assert_eq!(deserialized.submitted_at, 1719000000000);
    }

    #[test]
    fn test_command_envelope_missing_optionals() {
        let cmd = CommandEnvelope {
            request_id: "req-002".into(),
            method: "session.input".into(),
            params: json!({ "content": "hello" }),
            actor: Actor::with_id(ActorKind::User, "alice"),
            session_id: Some("sess-001".into()),
            idempotency_key: None,
            submitted_at: 1719000000001,
        };

        let json = serde_json::to_string(&cmd).unwrap();
        assert!(
            !json.contains("idempotency_key"),
            "optional field should be skipped when None"
        );
        assert!(json.contains(r#""session_id":"sess-001""#));

        let deserialized: CommandEnvelope = serde_json::from_str(&json).unwrap();
        assert!(deserialized.idempotency_key.is_none());
        assert_eq!(deserialized.session_id.unwrap(), "sess-001");
        assert_eq!(deserialized.actor.id.unwrap(), "alice");
    }
}
