use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Stable error codes — serialized as SCREAMING_SNAKE_CASE strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    SessionNotFound,
    SessionAlreadyTerminal,
    TaskNotFound,
    TaskAlreadyTerminal,
    InvalidTransition,
    PermissionDenied,
    ToolNotFound,
    ToolAlreadyTerminal,
    Internal,
    Serialization,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SessionNotFound => write!(f, "SessionNotFound"),
            Self::SessionAlreadyTerminal => write!(f, "SessionAlreadyTerminal"),
            Self::TaskNotFound => write!(f, "TaskNotFound"),
            Self::TaskAlreadyTerminal => write!(f, "TaskAlreadyTerminal"),
            Self::InvalidTransition => write!(f, "InvalidTransition"),
            Self::PermissionDenied => write!(f, "PermissionDenied"),
            Self::ToolNotFound => write!(f, "ToolNotFound"),
            Self::ToolAlreadyTerminal => write!(f, "ToolAlreadyTerminal"),
            Self::Internal => write!(f, "Internal"),
            Self::Serialization => write!(f, "Serialization"),
        }
    }
}

/// Protocol-level error envelope with stable code, human message, optional details, and retryability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    pub retryable: bool,
}

impl CoreError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
            retryable: false,
        }
    }

    pub fn with_details(
        code: ErrorCode,
        message: impl Into<String>,
        details: Value,
        retryable: bool,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            details: Some(details),
            retryable,
        }
    }

    // === Convenience constructors ===

    pub fn session_not_found(id: impl Into<String>) -> Self {
        let id = id.into();
        Self::new(
            ErrorCode::SessionNotFound,
            format!("Session not found: {id}"),
        )
    }

    pub fn session_already_terminal(id: impl Into<String>) -> Self {
        let id = id.into();
        Self::new(
            ErrorCode::SessionAlreadyTerminal,
            format!("Session already terminal: {id}"),
        )
    }

    pub fn task_not_found(id: impl Into<String>) -> Self {
        let id = id.into();
        Self::new(ErrorCode::TaskNotFound, format!("Task not found: {id}"))
    }

    pub fn task_already_terminal(id: impl Into<String>) -> Self {
        let id = id.into();
        Self::new(
            ErrorCode::TaskAlreadyTerminal,
            format!("Task already terminal: {id}"),
        )
    }

    pub fn invalid_transition(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::InvalidTransition,
            format!("Invalid transition: {} -> {}", from.into(), to.into()),
        )
    }

    pub fn permission_denied(reason: impl Into<String>) -> Self {
        Self::new(ErrorCode::PermissionDenied, reason)
    }

    pub fn tool_not_found(name: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::ToolNotFound,
            format!("Tool not found: {}", name.into()),
        )
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::Internal,
            message: msg.into(),
            details: None,
            retryable: true,
        }
    }

    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Serialization, msg)
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for CoreError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_core_error_round_trip() {
        let err = CoreError::session_not_found("sess-001");
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: CoreError = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.code, ErrorCode::SessionNotFound);
        assert!(deserialized.message.contains("sess-001"));
        assert!(!deserialized.retryable);
    }

    #[test]
    fn test_core_error_with_details() {
        let err = CoreError::with_details(
            ErrorCode::InvalidTransition,
            "Cannot transition from running to idle",
            json!({ "from": "running", "to": "idle" }),
            false,
        );
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: CoreError = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.code, ErrorCode::InvalidTransition);
        assert!(deserialized.details.is_some());
        assert!(!deserialized.retryable);
    }

    #[test]
    fn test_internal_error_is_retryable() {
        let err = CoreError::internal("db connection failed");
        assert!(err.retryable);
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: CoreError = serde_json::from_str(&json).unwrap();
        assert!(deserialized.retryable);
    }

    #[test]
    fn test_error_code_serialization() {
        let err = CoreError::session_not_found("x");
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains(r#""SESSION_NOT_FOUND""#));
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(ErrorCode::SessionNotFound.to_string(), "SessionNotFound");
        assert_eq!(ErrorCode::Internal.to_string(), "Internal");
    }
}
