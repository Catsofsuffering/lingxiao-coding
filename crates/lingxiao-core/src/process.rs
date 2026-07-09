use crate::persistence::DbOwner;
use rusqlite::{params, OptionalExtension, Result};
use serde_json::{json, Value};
use std::process::{Child, Command};
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
            match kill_pid_tree(process.pid) {
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

    /// Reconcile the `owned_processes` registry by marking rows whose underlying
    /// PID has already exited as terminal, **without ever killing a live
    /// process**.
    ///
    /// This is the safe periodic counterpart to [`cleanup_orphans`], which is
    /// only correct at daemon boot (where every `active` row is stale because
    /// the owning daemon is gone). Calling `cleanup_orphans` periodically would
    /// `kill_pid_tree` *every* active row — including the daemon's own live
    /// sidecars, terminals, REPLs, and MCP servers — so it must never be used
    /// as a recurring sweep.
    ///
    /// `reconcile_orphans` instead probes each `active` row's PID for liveness
    /// via [`pid_is_alive`] (a non-killing probe: `kill(pid, 0)` on Unix,
    /// `OpenProcess` + `GetExitCodeProcess` on Windows). When the PID is gone,
    /// the row is flipped to `reconciled_dead` with a `cleanup_attempted_at`
    /// timestamp and a short `last_error` note; live PIDs are left untouched.
    /// This mirrors the TS `PidRegistry.listAll` lazy reconciliation
    /// (`isSamePidEntry` → `processExists`) that silently drops dead-PID
    /// entries on read rather than killing live workers.
    ///
    /// Returns a report of how many `active` rows were inspected and how many
    /// were marked dead. A row whose PID is alive stays `active` and is not
    /// counted as `cleaned` — it is the daemon's own live managed process.
    pub fn reconcile_orphans(&self) -> Result<CleanupReport> {
        let active = self.active()?;
        let mut report = CleanupReport::default();
        for process in active {
            report.attempted += 1;
            if pid_is_alive(process.pid) {
                // Live PID owned by this (or a sibling) daemon process — must
                // not be killed. Leave it `active` so the owning manager can
                // still `complete`/`mark_failed` it through the normal path.
                continue;
            }
            self.db.conn().execute(
                "UPDATE owned_processes SET status = 'reconciled_dead', \
                 cleanup_attempted_at = ?1, last_error = ?2 WHERE id = ?3",
                params![
                    now_s(),
                    "periodic reconcile: pid no longer alive",
                    process.id
                ],
            )?;
            report.cleaned += 1;
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

pub fn configure_command_for_process_tree(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
    }
    #[cfg(not(unix))]
    {
        let _ = command;
    }
}

pub fn kill_child_tree(child: &mut Child) -> std::result::Result<(), String> {
    kill_pid_tree(child.id()).or_else(|tree_error| {
        child
            .kill()
            .map_err(|error| format!("{tree_error}; fallback child kill failed: {error}"))
    })
}

/// Non-killing liveness probe: returns `true` if a process with `pid` is
/// currently running, `false` if it has exited (or the PID is otherwise not a
/// live process).
///
/// Unlike [`kill_pid_tree`], this never sends a signal that could terminate the
/// target. On Unix it sends signal `0` (existence check only); on Windows it
/// opens a process handle for query access and reads `GetExitCodeProcess`,
/// treating `STILL_ACTIVE` (`259`) as alive. Used by the periodic
/// [`ProcessRegistry::reconcile_orphans`] sweep so a recurring cleanup can mark
/// dead-PID rows terminal without risking the daemon's own live managed
/// processes.
pub fn pid_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // `kill(pid, 0)` is the POSIX existence probe: it delivers no signal,
        // returning 0 if `pid` is a live process (or process group). `ESRCH`
        // means the process is gone. `EPERM` (a live process we may not signal)
        // is treated as alive — the process exists, we just lack permission,
        // which is the conservative/safe answer for a reconcile sweep.
        unsafe {
            libc::kill(pid as libc::pid_t, 0) == 0
                || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        }
    }
    #[cfg(not(unix))]
    {
        pid_alive_windows(pid)
    }
}

#[cfg(windows)]
fn pid_alive_windows(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    // SAFETY: `OpenProcess` returns a raw handle for query access only
    // (PROCESS_QUERY_LIMITED_INFORMATION grants no terminate rights, so even a
    // bug here cannot kill the process). We always close the handle. If the
    // process does not exist, `OpenProcess` returns NULL and we report dead.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle == 0 {
            return false;
        }
        let mut exit_code: u32 = 0;
        let ok = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);
        // `GetExitCodeProcess` returns 0 on failure (e.g. handle became
        // invalid between open/read). Treat that as not-alive so a race where
        // the process exited mid-probe reconciles the row rather than
        // leaving it `active` forever.
        ok != 0 && exit_code == STILL_ACTIVE as u32
    }
}

pub fn kill_pid_tree(pid: u32) -> std::result::Result<(), String> {
    #[cfg(target_os = "windows")]
    let output = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .output()
        .map_err(|e| format!("taskkill spawn failed: {e}"))?;

    #[cfg(target_os = "windows")]
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

    #[cfg(not(target_os = "windows"))]
    {
        kill_unix_process_tree(pid as libc::pid_t)
    }
}

#[cfg(not(target_os = "windows"))]
#[derive(Clone, Copy)]
enum UnixKillTarget {
    ProcessGroup(libc::pid_t),
    Process(libc::pid_t),
}

#[cfg(not(target_os = "windows"))]
fn kill_unix_process_tree(pid: libc::pid_t) -> std::result::Result<(), String> {
    let target = unsafe {
        if libc::kill(-pid, libc::SIGTERM) == 0 {
            UnixKillTarget::ProcessGroup(pid)
        } else {
            let group_error = std::io::Error::last_os_error();
            if libc::kill(pid, libc::SIGTERM) == 0 {
                UnixKillTarget::Process(pid)
            } else {
                let pid_error = std::io::Error::last_os_error();
                return Err(format!(
                    "process group kill failed: {group_error}; pid kill failed: {pid_error}"
                ));
            }
        }
    };

    if wait_unix_target_gone(target, std::time::Duration::from_millis(500)) {
        return Ok(());
    }

    unsafe {
        if unix_kill(target, libc::SIGKILL) == 0 || !unix_target_alive(target) {
            Ok(())
        } else {
            Err(format!(
                "process tree SIGKILL failed after SIGTERM: {}",
                std::io::Error::last_os_error()
            ))
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn wait_unix_target_gone(target: UnixKillTarget, timeout: std::time::Duration) -> bool {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if !unsafe { unix_target_alive(target) } {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    !unsafe { unix_target_alive(target) }
}

#[cfg(not(target_os = "windows"))]
unsafe fn unix_kill(target: UnixKillTarget, signal: libc::c_int) -> libc::c_int {
    match target {
        UnixKillTarget::ProcessGroup(pid) => libc::kill(-pid, signal),
        UnixKillTarget::Process(pid) => libc::kill(pid, signal),
    }
}

#[cfg(not(target_os = "windows"))]
unsafe fn unix_target_alive(target: UnixKillTarget) -> bool {
    if unix_kill(target, 0) == 0 {
        return true;
    }
    let error = std::io::Error::last_os_error();
    error.raw_os_error() != Some(libc::ESRCH)
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
    use std::time::{Duration, Instant};

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

    #[test]
    fn test_process_registry_cleanup_kills_descendant_process() {
        let pid_file = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut child = spawn_parent_with_sleeping_descendant(&pid_file);
        let child_pid = wait_for_pid_file(&pid_file);
        assert!(
            pid_alive(child_pid),
            "descendant should be alive before cleanup"
        );

        let registry = setup();
        registry
            .register("tree-parent", child.id(), "test", "owner", "tree")
            .unwrap();
        let report = registry.cleanup_orphans().unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.cleaned, 1);
        let _ = child.wait();
        wait_until_not_alive(child_pid);
        assert!(
            !pid_alive(child_pid),
            "cleanup_orphans must terminate descendant process {child_pid}"
        );
    }

    #[test]
    fn test_pid_is_alive_detects_live_and_dead() {
        // A PID that does not exist is reported dead (probe must not panic on a
        // bogus high PID — the reconcile sweep will hit many of these).
        assert!(!pid_is_alive(u32::MAX - 1), "bogus pid should not be alive");
        // The current process is alive.
        let me = std::process::id();
        assert!(pid_is_alive(me), "current process should be alive");
    }

    #[test]
    fn test_reconcile_orphans_marks_dead_pid_without_killing_live() {
        let registry = setup();
        // A live, owned child must be left `active` (never killed by the
        // reconcile sweep — that is the boot-time cleanup_orphans hazard this
        // method exists to avoid).
        let mut live = spawn_sleeping_child();
        let live_pid = live.id();
        registry
            .register("live-child", live_pid, "test", "owner", "live")
            .unwrap();
        // A bogus (already-dead) PID row should be reconciled to `reconciled_dead`.
        registry
            .register("dead-bogus", u32::MAX - 1, "test", "owner", "dead")
            .unwrap();

        let report = registry.reconcile_orphans().unwrap();
        assert_eq!(report.attempted, 2, "both active rows inspected");
        assert_eq!(report.cleaned, 1, "only the dead-PID row reconciled");
        // Live child stays `active` and is still running — the sweep never killed it.
        assert_eq!(
            registry.get("live-child").unwrap().unwrap().status,
            "active",
            "live managed process must not be touched by reconcile"
        );
        assert!(
            pid_alive(live_pid),
            "live child must still be alive after reconcile"
        );
        assert_eq!(
            registry.get("dead-bogus").unwrap().unwrap().status,
            "reconciled_dead",
            "dead-PID row must be marked reconciled_dead"
        );

        // Running reconcile again is a no-op now that the dead row is terminal.
        let again = registry.reconcile_orphans().unwrap();
        assert_eq!(again.attempted, 1, "only the live row remains active");
        assert_eq!(again.cleaned, 0, "nothing new to reconcile");

        // Clean up the live child we kept around (use kill_pid_tree, not the
        // sweep) so the test does not leak a sleeping process.
        let _ = kill_pid_tree(live_pid);
        let _ = live.wait();
    }

    #[test]
    fn test_reconcile_orphans_empty_is_noop() {
        let registry = setup();
        let report = registry.reconcile_orphans().unwrap();
        assert_eq!(report, CleanupReport::default());
    }

    #[test]
    fn test_reconcile_orphans_marks_reaped_real_child_as_dead() {
        // A real child that exits naturally (not killed) must be detected as
        // dead by the liveness probe on the next reconcile tick.
        let registry = setup();
        let before = std::process::Command::new(if cfg!(windows) { "cmd" } else { "sh" });
        let mut cmd = before;
        if cfg!(windows) {
            cmd.args(["/C", "exit 0"]);
        } else {
            cmd.args(["-c", "true"]);
        }
        let mut child = cmd.spawn().unwrap();
        let pid = child.id();
        registry
            .register("short-lived", pid, "test", "owner", "exits-0")
            .unwrap();
        // Wait for it to exit on its own.
        child.wait().unwrap();
        wait_until_not_alive(pid);

        let report = registry.reconcile_orphans().unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.cleaned, 1, "naturally-exited child reconciled");
        assert_eq!(
            registry.get("short-lived").unwrap().unwrap().status,
            "reconciled_dead"
        );
    }

    fn spawn_sleeping_child() -> std::process::Child {
        #[cfg(target_os = "windows")]
        {
            let powershell = resolve_powershell();
            let mut command = Command::new(powershell);
            command
                .args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            configure_command_for_process_tree(&mut command);
            command.spawn().unwrap()
        }
        #[cfg(not(target_os = "windows"))]
        {
            let mut command = Command::new("sh");
            command
                .args(["-c", "sleep 30"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            configure_command_for_process_tree(&mut command);
            command.spawn().unwrap()
        }
    }

    fn spawn_parent_with_sleeping_descendant(pid_file: &std::path::Path) -> std::process::Child {
        #[cfg(target_os = "windows")]
        {
            let powershell = resolve_powershell();
            let escaped = pid_file.display().to_string().replace('\'', "''");
            let script = format!(
                "$child = Start-Process -FilePath '{}' -ArgumentList '-NoProfile','-Command','Start-Sleep -Seconds 30' -PassThru; Set-Content -LiteralPath '{}' -Value $child.Id; Start-Sleep -Seconds 30",
                powershell.display(),
                escaped
            );
            let mut command = Command::new(powershell);
            command
                .args(["-NoProfile", "-Command", &script])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            configure_command_for_process_tree(&mut command);
            command.spawn().unwrap()
        }
        #[cfg(not(target_os = "windows"))]
        {
            let mut command = Command::new("sh");
            command
                .args([
                    "-c",
                    "sleep 30 & echo $! > \"$1\"; wait",
                    "sh",
                    &pid_file.display().to_string(),
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            configure_command_for_process_tree(&mut command);
            command.spawn().unwrap()
        }
    }

    fn wait_for_pid_file(path: &std::path::Path) -> u32 {
        let started = Instant::now();
        loop {
            if let Ok(text) = std::fs::read_to_string(path) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    return pid;
                }
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "timed out waiting for descendant pid file {}",
                path.display()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_until_not_alive(pid: u32) {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(5) {
            if !pid_alive(pid) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn pid_alive(pid: u32) -> bool {
        #[cfg(target_os = "windows")]
        {
            let powershell = resolve_powershell();
            Command::new(powershell)
                .args([
                    "-NoProfile",
                    "-Command",
                    &format!("if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 0 }} else {{ exit 1 }}"),
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
        }
        #[cfg(not(target_os = "windows"))]
        {
            unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
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
