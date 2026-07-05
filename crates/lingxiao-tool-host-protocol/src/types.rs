use serde::{Deserialize, Serialize};

pub type RequestId = String;

/// Serialisable cancel token.
///
/// The `token_id` identifies a shared cancellation channel between Core and
/// sidecar.  Core closes the channel when cancellation is requested; the
/// sidecar MUST check this token periodically and abort promptly.
///
/// # Boundary promise
/// - No DB handle, no canonical state mutation reference.
/// - Transport layer (not this type) wires the actual cancel mechanism.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelToken {
    pub token_id: String,
}

/// Lease that authorises the sidecar to write to the workspace filesystem.
///
/// # Fields
/// - `scope` — allowed path prefix (e.g. `"/workspace/project/src"`).
/// - `generation` — monotonic counter used by Core to invalidate stale leases.
/// - `expires_at` — Unix timestamp (milliseconds) after which the lease is
///   no longer valid.
///
/// # Boundary promise
/// - No DB handle, no canonical state mutation.
/// - Sidecar MUST check `scope` and `expires_at` before every write.
/// - Core MUST reject writes that fall outside the lease scope or expiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionLease {
    pub scope: String,
    pub generation: u64,
    pub expires_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceBudget {
    pub max_runtime_ms: u64,
    pub max_memory_mb: u64,
    pub max_cpu_ms: u64,
    pub max_network_bytes: u64,
    pub max_file_write_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub runtime_ms: u64,
    pub cpu_ms: u64,
    pub memory_mb_peak: u64,
    pub network_bytes: u64,
    pub file_write_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── CancelToken ──────────────────────────────────────────────────────

    #[test]
    fn test_cancel_token_round_trip() {
        let orig = CancelToken {
            token_id: "ct_abc123".into(),
        };
        let json = serde_json::to_string(&orig).unwrap();
        let restored: CancelToken = serde_json::from_str(&json).unwrap();
        assert_eq!(orig.token_id, restored.token_id);
    }

    #[test]
    fn test_cancel_token_field_name() {
        let json = r#"{"token_id":"ct_xyz"}"#;
        let ct: CancelToken = serde_json::from_str(json).unwrap();
        assert_eq!(ct.token_id, "ct_xyz");
    }

    // ── PermissionLease ──────────────────────────────────────────────────

    #[test]
    fn test_permission_lease_round_trip() {
        let orig = PermissionLease {
            scope: "/workspace/project/src".into(),
            generation: 42,
            expires_at: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&orig).unwrap();
        let restored: PermissionLease = serde_json::from_str(&json).unwrap();
        assert_eq!(orig.scope, restored.scope);
        assert_eq!(orig.generation, restored.generation);
        assert_eq!(orig.expires_at, restored.expires_at);
    }

    #[test]
    fn test_permission_lease_snake_case_fields() {
        let json = r#"{"scope":"/tmp","generation":1,"expires_at":9999}"#;
        let p: PermissionLease = serde_json::from_str(json).unwrap();
        assert_eq!(p.scope, "/tmp");
        assert_eq!(p.generation, 1);
        assert_eq!(p.expires_at, 9999);
    }

    #[test]
    fn test_permission_lease_minimal_values() {
        let orig = PermissionLease {
            scope: String::new(),
            generation: 0,
            expires_at: 0,
        };
        let json = serde_json::to_string(&orig).unwrap();
        let restored: PermissionLease = serde_json::from_str(&json).unwrap();
        assert!(restored.scope.is_empty());
        assert_eq!(restored.generation, 0);
    }

    // ── ResourceBudget ───────────────────────────────────────────────────

    #[test]
    fn test_resource_budget_round_trip() {
        let orig = ResourceBudget {
            max_runtime_ms: 120_000,
            max_memory_mb: 512,
            max_cpu_ms: 120_000,
            max_network_bytes: 10_485_760,
            max_file_write_bytes: 52_428_800,
        };
        let json = serde_json::to_string(&orig).unwrap();
        let restored: ResourceBudget = serde_json::from_str(&json).unwrap();
        assert_eq!(orig.max_runtime_ms, restored.max_runtime_ms);
        assert_eq!(orig.max_memory_mb, restored.max_memory_mb);
        assert_eq!(orig.max_cpu_ms, restored.max_cpu_ms);
        assert_eq!(orig.max_network_bytes, restored.max_network_bytes);
        assert_eq!(orig.max_file_write_bytes, restored.max_file_write_bytes);
    }

    #[test]
    fn test_resource_budget_snake_case_fields() {
        let json = r#"{
            "max_runtime_ms": 1000,
            "max_memory_mb": 256,
            "max_cpu_ms": 1000,
            "max_network_bytes": 1024,
            "max_file_write_bytes": 4096
        }"#;
        let b: ResourceBudget = serde_json::from_str(json).unwrap();
        assert_eq!(b.max_runtime_ms, 1000);
        assert_eq!(b.max_memory_mb, 256);
    }

    // ── ResourceUsage ────────────────────────────────────────────────────

    #[test]
    fn test_resource_usage_round_trip() {
        let orig = ResourceUsage {
            runtime_ms: 5_432,
            cpu_ms: 3_210,
            memory_mb_peak: 128,
            network_bytes: 2_097_152,
            file_write_bytes: 65_536,
        };
        let json = serde_json::to_string(&orig).unwrap();
        let restored: ResourceUsage = serde_json::from_str(&json).unwrap();
        assert_eq!(orig.runtime_ms, restored.runtime_ms);
        assert_eq!(orig.cpu_ms, restored.cpu_ms);
        assert_eq!(orig.memory_mb_peak, restored.memory_mb_peak);
        assert_eq!(orig.network_bytes, restored.network_bytes);
        assert_eq!(orig.file_write_bytes, restored.file_write_bytes);
    }

    #[test]
    fn test_resource_usage_zero_values() {
        let orig = ResourceUsage::default();
        let json = serde_json::to_string(&orig).unwrap();
        let restored: ResourceUsage = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.runtime_ms, 0);
    }

    // ── No DB / canonical-state fields ───────────────────────────────────

    /// Guard: public field names MUST NOT contain `db` or `state`.
    /// (Case-insensitive check on the struct field declarations.)
    #[test]
    fn test_no_db_or_state_fields_in_protocol_types() {
        let source = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/types.rs"),
        )
        .expect("read types.rs");

        for line in source.lines() {
            let trimmed = line.trim();
            // Skip comments, doc attrs, non-field lines
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
