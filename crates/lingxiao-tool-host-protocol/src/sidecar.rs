use crate::types::*;
use serde::{Deserialize, Serialize};

/// Envelope sent from Rust Core to a sidecar process.
///
/// # Boundary promises
/// - **No DB handle** — this struct carries no database connection,
///   connection string, or path to a SQLite file.
/// - **No canonical state mutation** — it carries no reference to
///   in-memory session/task state; the sidecar cannot mutate Core state.
/// - **File writes require a `PermissionLease`** — when present, the sidecar
///   MUST only write within the lease scope and MUST respect its expiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarRequest {
    // === Routing ===
    pub request_id: RequestId,
    pub session_id: String,
    pub task_id: Option<String>,
    pub agent_id: String,

    // === Tool invocation ===
    /// Core tool call lifecycle identity (distinct from `request_id`).
    /// Used for cancel/timeout/resource attribution in the Core scheduler.
    pub tool_call_id: String,
    pub tool_name: String,
    pub args: Vec<u8>,

    // === Capability negotiation ===
    pub capabilities: Vec<String>,

    // === Lifecycle ===
    /// Hard deadline as a Unix timestamp in milliseconds.
    /// Core will close the cancel token after this point.
    pub deadline: i64,

    // === Resource budget ===
    pub resource_budget: ResourceBudget,

    // === Cancellation ===
    /// Token the sidecar MUST monitor; when closed, abort immediately.
    pub cancel_token: CancelToken,

    // === Permission lease (optional) ===
    /// When present, authorises writes within a specific path scope.
    pub permission_lease: Option<PermissionLease>,
}

/// Typed response from a sidecar back to Rust Core.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SidecarResponse {
    Completed(SidecarCompleted),
    Stream(SidecarStreamChunk),
    Progress(SidecarProgress),
    Error(SidecarError),
}

/// Successful completion payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarCompleted {
    pub request_id: RequestId,
    pub result: Vec<u8>,
    pub result_shape: String,
    pub duration_ms: u64,
    pub usage: ResourceUsage,
}

/// A single chunk in a streaming response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarStreamChunk {
    pub request_id: RequestId,
    pub sequence: u64,
    pub data: Vec<u8>,
    pub is_final: bool,
    pub usage: Option<ResourceUsage>,
}

/// Interim progress update during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarProgress {
    pub request_id: RequestId,
    pub percent: Option<f64>,
    pub message: Option<String>,
    pub usage: ResourceUsage,
}

/// Fatal / terminal error from the sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarError {
    pub request_id: RequestId,
    pub code: SidecarErrorCode,
    pub message: String,
    pub details: Option<Vec<u8>>,
    pub usage: ResourceUsage,
}

/// Error classification for sidecar operations.
///
/// Serialised as **snake_case** in JSON for protocol consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SidecarErrorCode {
    // === Caller errors ===
    InvalidArgs,
    UnsupportedTool,
    CapabilityMismatch,

    // === Execution errors ===
    RuntimeError,
    ToolExecutionFailed,
    ResourceExhausted,

    // === Lifecycle ===
    Cancelled,
    Timeout,
    HeartbeatLost,
    Crash,

    // === Permission ===
    PermissionDenied,
    FileWriteDenied,
    DbWriteDetected,

    // === Communication ===
    ProtocolError,
    TransportError,

    // === Internal ===
    /// Catch-all for unexpected sidecar-internal failures that do not fit
    /// any other category.  Core treats this as non-retryable by default.
    Internal,
}

/// Periodic heartbeat from the sidecar to report health and resource usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarHeartbeat {
    pub request_id: RequestId,
    pub usage: ResourceUsage,
    pub health: HeartbeatHealth,
}

/// Self-assessed health status carried in every heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeartbeatHealth {
    Healthy,
    Warning { reason: String },
    Critical { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────────

    fn sample_usage() -> ResourceUsage {
        ResourceUsage {
            runtime_ms: 1_234,
            cpu_ms: 567,
            memory_mb_peak: 256,
            network_bytes: 4_096,
            file_write_bytes: 1_024,
        }
    }

    fn sample_budget() -> ResourceBudget {
        ResourceBudget {
            max_runtime_ms: 120_000,
            max_memory_mb: 512,
            max_cpu_ms: 120_000,
            max_network_bytes: 10_485_760,
            max_file_write_bytes: 52_428_800,
        }
    }

    fn sample_lease() -> PermissionLease {
        PermissionLease {
            scope: "/workspace/project".into(),
            generation: 1,
            expires_at: 1_800_000_000_000,
        }
    }

    fn sample_token() -> CancelToken {
        CancelToken {
            token_id: "ct_req_001".into(),
        }
    }

    fn sample_request() -> SidecarRequest {
        SidecarRequest {
            request_id: "req_001".into(),
            session_id: "ses_abc".into(),
            task_id: Some("task_42".into()),
            agent_id: "agent_explore_1".into(),
            tool_call_id: "tc_9876".into(),
            tool_name: "browser_action".into(),
            args: br#"{"action":"navigate","url":"https://example.com"}"#.to_vec(),
            capabilities: vec!["browser".into(), "network".into()],
            deadline: 1_800_000_000_000,
            resource_budget: sample_budget(),
            cancel_token: sample_token(),
            permission_lease: Some(sample_lease()),
        }
    }

    // ── SidecarRequest ───────────────────────────────────────────────────

    #[test]
    fn test_request_round_trip() {
        let req = sample_request();
        let json = serde_json::to_string_pretty(&req).unwrap();
        let restored: SidecarRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req.request_id, restored.request_id);
        assert_eq!(req.session_id, restored.session_id);
        assert_eq!(req.task_id, restored.task_id);
        assert_eq!(req.agent_id, restored.agent_id);
        assert_eq!(req.tool_call_id, restored.tool_call_id);
        assert_eq!(req.tool_name, restored.tool_name);
        assert_eq!(req.args, restored.args);
        assert_eq!(req.capabilities, restored.capabilities);
        assert_eq!(req.deadline, restored.deadline);
        assert_eq!(
            req.resource_budget.max_runtime_ms,
            restored.resource_budget.max_runtime_ms
        );
        assert_eq!(req.cancel_token.token_id, restored.cancel_token.token_id);
        assert_eq!(
            req.permission_lease.as_ref().unwrap().scope,
            restored.permission_lease.unwrap().scope
        );
    }

    #[test]
    fn test_request_without_optional_lease() {
        let req = SidecarRequest {
            permission_lease: None,
            ..sample_request()
        };
        let json = serde_json::to_string(&req).unwrap();
        let restored: SidecarRequest = serde_json::from_str(&json).unwrap();
        assert!(restored.permission_lease.is_none());
    }

    #[test]
    fn test_request_without_optional_task_id() {
        let req = SidecarRequest {
            task_id: None,
            ..sample_request()
        };
        let json = serde_json::to_string(&req).unwrap();
        let restored: SidecarRequest = serde_json::from_str(&json).unwrap();
        assert!(restored.task_id.is_none());
    }

    #[test]
    fn test_request_tool_call_id_present() {
        let json = serde_json::to_string(&sample_request()).unwrap();
        let restored: SidecarRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.tool_call_id, "tc_9876");
        assert!(json.contains("tool_call_id"));
    }

    #[test]
    fn test_request_cancel_token_present() {
        let json = serde_json::to_string(&sample_request()).unwrap();
        assert!(json.contains("cancel_token"));
        assert!(json.contains("token_id"));
    }

    #[test]
    fn test_request_permission_lease_fields_present() {
        let json = serde_json::to_string(&sample_request()).unwrap();
        assert!(json.contains("permission_lease"));
        assert!(json.contains("scope"));
        assert!(json.contains("generation"));
        assert!(json.contains("expires_at"));
    }

    // ── SidecarResponse ──────────────────────────────────────────────────

    #[test]
    fn test_response_completed_round_trip() {
        let resp = SidecarResponse::Completed(SidecarCompleted {
            request_id: "req_001".into(),
            result: br#"{"status":"ok"}"#.to_vec(),
            result_shape: "json".into(),
            duration_ms: 1_234,
            usage: sample_usage(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        let restored: SidecarResponse = serde_json::from_str(&json).unwrap();
        match restored {
            SidecarResponse::Completed(c) => {
                assert_eq!(c.request_id, "req_001");
                assert_eq!(c.result_shape, "json");
                assert_eq!(c.duration_ms, 1_234);
                assert_eq!(c.usage.runtime_ms, 1_234);
            }
            _ => panic!("expected Completed variant"),
        }
    }

    #[test]
    fn test_response_stream_round_trip() {
        let resp = SidecarResponse::Stream(SidecarStreamChunk {
            request_id: "req_001".into(),
            sequence: 3,
            data: b"chunk data".to_vec(),
            is_final: false,
            usage: None,
        });
        let json = serde_json::to_string(&resp).unwrap();
        let restored: SidecarResponse = serde_json::from_str(&json).unwrap();
        match restored {
            SidecarResponse::Stream(s) => {
                assert_eq!(s.sequence, 3);
                assert!(!s.is_final);
                assert!(s.usage.is_none());
            }
            _ => panic!("expected Stream variant"),
        }
    }

    #[test]
    fn test_response_stream_final_with_usage() {
        let resp = SidecarResponse::Stream(SidecarStreamChunk {
            request_id: "req_001".into(),
            sequence: 99,
            data: b"final".to_vec(),
            is_final: true,
            usage: Some(sample_usage()),
        });
        let json = serde_json::to_string(&resp).unwrap();
        let restored: SidecarResponse = serde_json::from_str(&json).unwrap();
        match restored {
            SidecarResponse::Stream(s) => {
                assert!(s.is_final);
                assert_eq!(s.usage.unwrap().runtime_ms, 1_234);
            }
            _ => panic!("expected Stream variant"),
        }
    }

    #[test]
    fn test_response_progress_round_trip() {
        let resp = SidecarResponse::Progress(SidecarProgress {
            request_id: "req_001".into(),
            percent: Some(0.75),
            message: Some("processing page 3 of 4".into()),
            usage: sample_usage(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        let restored: SidecarResponse = serde_json::from_str(&json).unwrap();
        match restored {
            SidecarResponse::Progress(p) => {
                assert!((p.percent.unwrap() - 0.75).abs() < 1e-10);
                assert_eq!(p.message.unwrap(), "processing page 3 of 4");
            }
            _ => panic!("expected Progress variant"),
        }
    }

    #[test]
    fn test_response_progress_without_percent_or_message() {
        let resp = SidecarResponse::Progress(SidecarProgress {
            request_id: "req_001".into(),
            percent: None,
            message: None,
            usage: sample_usage(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        let restored: SidecarResponse = serde_json::from_str(&json).unwrap();
        match restored {
            SidecarResponse::Progress(p) => {
                assert!(p.percent.is_none());
                assert!(p.message.is_none());
            }
            _ => panic!("expected Progress variant"),
        }
    }

    #[test]
    fn test_response_error_round_trip() {
        let resp = SidecarResponse::Error(SidecarError {
            request_id: "req_001".into(),
            code: SidecarErrorCode::Timeout,
            message: "deadline exceeded".into(),
            details: Some(br#"{"timed_out_after_ms":100}"#.to_vec()),
            usage: sample_usage(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        let restored: SidecarResponse = serde_json::from_str(&json).unwrap();
        match restored {
            SidecarResponse::Error(e) => {
                assert!(matches!(e.code, SidecarErrorCode::Timeout));
                assert_eq!(e.message, "deadline exceeded");
                assert!(e.details.is_some());
            }
            _ => panic!("expected Error variant"),
        }
    }

    // ── SidecarErrorCode snake_case serialisation ────────────────────────

    #[test]
    fn test_error_code_serialises_snake_case() {
        // Each variant should produce snake_case JSON
        let cases: Vec<(SidecarErrorCode, &str)> = vec![
            (SidecarErrorCode::InvalidArgs, "invalid_args"),
            (SidecarErrorCode::UnsupportedTool, "unsupported_tool"),
            (SidecarErrorCode::CapabilityMismatch, "capability_mismatch"),
            (SidecarErrorCode::RuntimeError, "runtime_error"),
            (
                SidecarErrorCode::ToolExecutionFailed,
                "tool_execution_failed",
            ),
            (SidecarErrorCode::ResourceExhausted, "resource_exhausted"),
            (SidecarErrorCode::Cancelled, "cancelled"),
            (SidecarErrorCode::Timeout, "timeout"),
            (SidecarErrorCode::HeartbeatLost, "heartbeat_lost"),
            (SidecarErrorCode::Crash, "crash"),
            (SidecarErrorCode::PermissionDenied, "permission_denied"),
            (SidecarErrorCode::FileWriteDenied, "file_write_denied"),
            (SidecarErrorCode::DbWriteDetected, "db_write_detected"),
            (SidecarErrorCode::ProtocolError, "protocol_error"),
            (SidecarErrorCode::TransportError, "transport_error"),
            (SidecarErrorCode::Internal, "internal"),
        ];
        for (code, expected) in &cases {
            let json = serde_json::to_string(code).unwrap();
            // JSON string output: "\"cancelled\"" etc.
            let expected_json = format!("\"{}\"", expected);
            assert_eq!(json, expected_json, "mismatch for {:?}", code);
        }
    }

    #[test]
    fn test_error_code_deserialises_snake_case() {
        let json = r#""cancelled""#;
        let code: SidecarErrorCode = serde_json::from_str(json).unwrap();
        assert!(matches!(code, SidecarErrorCode::Cancelled));

        let json = r#""timeout""#;
        let code: SidecarErrorCode = serde_json::from_str(json).unwrap();
        assert!(matches!(code, SidecarErrorCode::Timeout));

        let json = r#""db_write_detected""#;
        let code: SidecarErrorCode = serde_json::from_str(json).unwrap();
        assert!(matches!(code, SidecarErrorCode::DbWriteDetected));
    }

    // ── HeartbeatHealth ──────────────────────────────────────────────────

    #[test]
    fn test_heartbeat_healthy_round_trip() {
        let hb = SidecarHeartbeat {
            request_id: "req_001".into(),
            usage: sample_usage(),
            health: HeartbeatHealth::Healthy,
        };
        let json = serde_json::to_string(&hb).unwrap();
        let restored: SidecarHeartbeat = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored.health, HeartbeatHealth::Healthy));
    }

    #[test]
    fn test_heartbeat_warning_round_trip() {
        let hb = SidecarHeartbeat {
            request_id: "req_001".into(),
            usage: sample_usage(),
            health: HeartbeatHealth::Warning {
                reason: "memory at 90%".into(),
            },
        };
        let json = serde_json::to_string(&hb).unwrap();
        let restored: SidecarHeartbeat = serde_json::from_str(&json).unwrap();
        match restored.health {
            HeartbeatHealth::Warning { reason } => {
                assert_eq!(reason, "memory at 90%");
            }
            _ => panic!("expected Warning variant"),
        }
    }

    #[test]
    fn test_heartbeat_critical_round_trip() {
        let hb = SidecarHeartbeat {
            request_id: "req_001".into(),
            usage: sample_usage(),
            health: HeartbeatHealth::Critical {
                reason: "OOM imminent".into(),
            },
        };
        let json = serde_json::to_string(&hb).unwrap();
        let restored: SidecarHeartbeat = serde_json::from_str(&json).unwrap();
        match restored.health {
            HeartbeatHealth::Critical { reason } => {
                assert_eq!(reason, "OOM imminent");
            }
            _ => panic!("expected Critical variant"),
        }
    }

    #[test]
    fn test_heartbeat_health_snake_case() {
        let json = r#"{"request_id":"r1","usage":{"runtime_ms":0,"cpu_ms":0,"memory_mb_peak":0,"network_bytes":0,"file_write_bytes":0},"health":"healthy"}"#;
        let hb: SidecarHeartbeat = serde_json::from_str(json).unwrap();
        assert!(matches!(hb.health, HeartbeatHealth::Healthy));

        let json = r#"{"request_id":"r1","usage":{"runtime_ms":0,"cpu_ms":0,"memory_mb_peak":0,"network_bytes":0,"file_write_bytes":0},"health":{"warning":{"reason":"test"}}}"#;
        let hb: SidecarHeartbeat = serde_json::from_str(json).unwrap();
        match hb.health {
            HeartbeatHealth::Warning { reason } => assert_eq!(reason, "test"),
            _ => panic!("expected Warning"),
        }
    }

    // ── No DB / canonical-state fields ───────────────────────────────────

    /// Guard: public field names in this file must not contain `db` or `state`.
    #[test]
    fn test_no_db_or_state_fields_in_sidecar_types() {
        let source = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sidecar.rs"),
        )
        .expect("read sidecar.rs");

        for line in source.lines() {
            let trimmed = line.trim();
            if !trimmed.starts_with("pub ") {
                continue;
            }
            let lower = trimmed.to_lowercase();
            assert!(
                !lower.contains("db_") && !lower.contains("state_"),
                "potential DB/state field found: {trimmed}"
            );
        }
    }
}
