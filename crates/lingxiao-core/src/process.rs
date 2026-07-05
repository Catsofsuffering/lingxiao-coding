use crate::persistence::DbOwner;
use rusqlite::{params, OptionalExtension, Result};
use serde_json::{json, Value};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedProcess {
    pub id: String,
    pub pid: u32,
    pub owner_kind: String,
    pub owner_id: String,
    pub label: String,
    pub status: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupReport {
    pub attempted: usize,
    pub cleaned: usize,
    pub failed: usize,
}

#[derive(Clone)]
pub struct ProcessRegistry {
    db: DbOwner,
}

impl ProcessRegistry {
    pub fn new(db: DbOwner) -> Self {
        Self { db }
    }

    pub fn register(
        &self,
        id: impl Into<String>,
        pid: u32,
        owner_kind: impl Into<String>,
        owner_id: impl Into<String>,
        label: impl Into<String>,
    ) -> Result<()> {
        let now = now_s();
        self.db.conn().execute(
            "INSERT INTO owned_processes \
             (id, pid, owner_kind, owner_id, label, status, started_at, completed_at, cleanup_attempted_at, exit_code, last_error) \
             VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, NULL, NULL, NULL, NULL) \
             ON CONFLICT(id) DO UPDATE SET \
             pid = excluded.pid, owner_kind = excluded.owner_kind, owner_id = excluded.owner_id, \
             label = excluded.label, status = 'active', started_at = excluded.started_at, \
             completed_at = NULL, cleanup_attempted_at = NULL, exit_code = NULL, last_error = NULL",
            params![id.into(), pid, owner_kind.into(), owner_id.into(), label.into(), now],
        )?;
        Ok(())
    }

    pub fn complete(&self, id: &str, exit_code: Option<i32>) -> Result<bool> {
        let updated = self.db.conn().execute(
            "UPDATE owned_processes SET status = 'completed', completed_at = ?1, exit_code = ?2, last_error = NULL \
             WHERE id = ?3 AND status = 'active'",
            params![now_s(), exit_code, id],
        )?;
        Ok(updated > 0)
    }

    pub fn mark_failed(&self, id: &str, error: &str) -> Result<bool> {
        let updated = self.db.conn().execute(
            "UPDATE owned_processes SET status = 'failed', completed_at = ?1, last_error = ?2 \
             WHERE id = ?3 AND status = 'active'",
            params![now_s(), error, id],
        )?;
        Ok(updated > 0)
    }

    pub fn active(&self) -> Result<Vec<OwnedProcess>> {
        let conn = self.db.conn();
        let mut stmt = conn.prepare(
            "SELECT id, pid, owner_kind, owner_id, label, status \
             FROM owned_processes WHERE status = 'active' ORDER BY started_at, id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(OwnedProcess {
                id: row.get(0)?,
                pid: row.get::<_, u32>(1)?,
                owner_kind: row.get(2)?,
                owner_id: row.get(3)?,
                label: row.get(4)?,
                status: row.get(5)?,
            })
        })?;
        rows.collect()
    }

    pub fn get(&self, id: &str) -> Result<Option<OwnedProcess>> {
        self.db
            .conn()
            .query_row(
                "SELECT id, pid, owner_kind, owner_id, label, status \
                 FROM owned_processes WHERE id = ?1",
                params![id],
                |row| {
                    Ok(OwnedProcess {
                        id: row.get(0)?,
                        pid: row.get::<_, u32>(1)?,
                        owner_kind: row.get(2)?,
                        owner_id: row.get(3)?,
                        label: row.get(4)?,
                        status: row.get(5)?,
                    })
                },
            )
            .optional()
    }

    pub fn cleanup_orphans(&self) -> Result<CleanupReport> {
        let active = self.active()?;
        let mut report = CleanupReport::default();
        for process in active {
            report.attempted += 1;
            match kill_pid(process.pid) {
                Ok(()) => {
                    report.cleaned += 1;
                    self.db.conn().execute(
                        "UPDATE owned_processes SET status = 'cleaned', cleanup_attempted_at = ?1, last_error = NULL \
                         WHERE id = ?2",
                        params![now_s(), process.id],
                    )?;
                }
                Err(error) => {
                    report.failed += 1;
                    self.db.conn().execute(
                        "UPDATE owned_processes SET status = 'cleanup_failed', cleanup_attempted_at = ?1, last_error = ?2 \
                         WHERE id = ?3",
                        params![now_s(), error, process.id],
                    )?;
                }
            }
        }
        Ok(report)
    }

    pub fn debug_rows(&self) -> Result<Vec<Value>> {
        let conn = self.db.conn();
        let mut stmt = conn.prepare(
            "SELECT id, pid, owner_kind, owner_id, label, status, started_at, completed_at, cleanup_attempted_at, exit_code, last_error \
             FROM owned_processes ORDER BY started_at, id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(json!({
                "id": row.get::<_, String>(0)?,
                "pid": row.get::<_, u32>(1)?,
                "owner_kind": row.get::<_, String>(2)?,
                "owner_id": row.get::<_, String>(3)?,
                "label": row.get::<_, String>(4)?,
                "status": row.get::<_, String>(5)?,
                "started_at": row.get::<_, f64>(6)?,
                "completed_at": row.get::<_, Option<f64>>(7)?,
                "cleanup_attempted_at": row.get::<_, Option<f64>>(8)?,
                "exit_code": row.get::<_, Option<i32>>(9)?,
                "last_error": row.get::<_, Option<String>>(10)?,
            }))
        })?;
        rows.collect()
    }
}

fn kill_pid(pid: u32) -> std::result::Result<(), String> {
    #[cfg(target_os = "windows")]
    let output = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .output()
        .map_err(|e| format!("taskkill spawn failed: {e}"))?;

    #[cfg(not(target_os = "windows"))]
    let output = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .output()
        .map_err(|e| format!("kill spawn failed: {e}"))?;

    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(stderr
            .lines()
            .next()
            .unwrap_or("process cleanup command failed")
            .trim()
            .to_string())
    }
}

fn now_s() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::DbOwner;
    use std::process::{Command, Stdio};

    fn setup() -> ProcessRegistry {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        ProcessRegistry::new(db)
    }

    #[test]
    fn test_process_registry_register_complete() {
        let registry = setup();
        registry
            .register("proc-1", 12345, "test", "owner-1", "unit process")
            .unwrap();
        let active = registry.active().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, "proc-1");
        assert!(registry.complete("proc-1", Some(0)).unwrap());
        assert!(registry.active().unwrap().is_empty());
        assert_eq!(registry.get("proc-1").unwrap().unwrap().status, "completed");
    }

    #[test]
    fn test_process_registry_cleanup_unknown_is_noop() {
        let registry = setup();
        let report = registry.cleanup_orphans().unwrap();
        assert_eq!(report, CleanupReport::default());
        assert!(!registry.complete("missing", None).unwrap());
    }

    #[test]
    fn test_process_registry_cleanup_bogus_owned_pid_is_safe_failure() {
        let registry = setup();
        registry
            .register("bogus", u32::MAX - 1, "test", "owner", "bogus")
            .unwrap();
        let report = registry.cleanup_orphans().unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.cleaned, 0);
        assert_eq!(report.failed, 1);
        assert_eq!(
            registry.get("bogus").unwrap().unwrap().status,
            "cleanup_failed"
        );
    }

    #[test]
    fn test_process_registry_cleanup_owned_child() {
        let mut child = spawn_sleeping_child();
        let pid = child.id();
        let registry = setup();
        registry
            .register("sleep-child", pid, "test", "owner", "sleep")
            .unwrap();

        let report = registry.cleanup_orphans().unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.cleaned, 1);
        assert_eq!(
            registry.get("sleep-child").unwrap().unwrap().status,
            "cleaned"
        );
        let _ = child.wait();
    }

    fn spawn_sleeping_child() -> std::process::Child {
        #[cfg(target_os = "windows")]
        {
            let powershell = resolve_powershell();
            Command::new(powershell)
                .args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        }
        #[cfg(not(target_os = "windows"))]
        {
            Command::new("sh")
                .args(["-c", "sleep 30"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        }
    }

    #[cfg(target_os = "windows")]
    fn resolve_powershell() -> std::path::PathBuf {
        for key in ["SystemRoot", "windir", "SYSTEMROOT", "WINDIR"] {
            if let Ok(root) = std::env::var(key) {
                let candidate = std::path::Path::new(&root)
                    .join("System32")
                    .join("WindowsPowerShell")
                    .join("v1.0")
                    .join("powershell.exe");
                if candidate.exists() {
                    return candidate;
                }
            }
        }
        std::path::PathBuf::from("powershell.exe")
    }
}
