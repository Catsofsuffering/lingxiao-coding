use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn daemon_bin() -> String {
    option_env!("CARGO_BIN_EXE_lingxiao_core_daemon")
        .unwrap_or("lingxiao-core-daemon")
        .to_string()
}

fn make_db_path() -> (std::path::PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let db = dir.path().join("test.db");
    (db, dir)
}

fn send_and_recv(
    stdin: &mut impl Write,
    reader: &mut BufReader<impl std::io::Read>,
    json_line: &str,
) -> serde_json::Value {
    writeln!(stdin, "{json_line}").expect("write stdin");
    stdin.flush().expect("flush stdin");
    let mut line = String::new();
    reader.read_line(&mut line).expect("read stdout");
    serde_json::from_str(&line).expect("parse response JSON")
}

#[test]
fn test_serve_version() {
    let output = Command::new(daemon_bin())
        .arg("version")
        .output()
        .expect("spawn daemon version");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("lingxiao-core-daemon"));
}

#[test]
fn test_serve_help() {
    let output = Command::new(daemon_bin())
        .arg("help")
        .output()
        .expect("spawn daemon help");
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("serve --db"));
}

#[test]
fn test_stdio_session_create_input_snapshot() {
    let (db_path, _dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    // GS-001 (Phase 1 subset): session.create → session.input → session.snapshot
    // Step 1: session.create
    let resp = send_and_recv(
        &mut stdin,
        &mut reader,
        r#"{"request_id":"req-1","method":"session.create","params":{"workspace":"/tmp/test-ws"},"actor":{"kind":"user"},"submitted_at":1000}"#,
    );
    assert_eq!(resp["success"], true, "session.create failed: {resp:?}");
    assert_eq!(resp["events"][0]["event_type"], "session.created");
    let session_id = resp["events"][0]["payload"]["session_id"]
        .as_str()
        .expect("session_id")
        .to_string();
    let create_seq = resp["latest_seq"].as_u64().unwrap_or(0);

    // Step 2: session.input
    let input_json = format!(
        r#"{{"request_id":"req-2","method":"session.input","params":{{"content":"完成调研报告"}},"actor":{{"kind":"user"}},"session_id":"{session_id}","submitted_at":1001}}"#
    );
    let resp = send_and_recv(&mut stdin, &mut reader, &input_json);
    assert_eq!(resp["success"], true, "session.input failed: {resp:?}");
    assert_eq!(resp["events"][0]["event_type"], "session.input_received");
    let input_seq = resp["latest_seq"].as_u64().unwrap_or(0);
    assert!(input_seq > create_seq, "seq should advance");

    // Step 3: session.snapshot
    let snap_json = format!(
        r#"{{"request_id":"req-3","method":"session.snapshot","params":{{}},"actor":{{"kind":"user"}},"session_id":"{session_id}","submitted_at":1002}}"#
    );
    let resp = send_and_recv(&mut stdin, &mut reader, &snap_json);
    assert_eq!(resp["success"], true, "session.snapshot failed: {resp:?}");
    let snap = &resp["result"];
    assert_eq!(snap["session_id"], session_id);
    assert_eq!(snap["last_seq"], input_seq);
    assert_eq!(snap["status"], "active");

    // Clean shutdown: close stdin → daemon sees EOF
    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success(), "daemon should exit 0 on EOF");
}

#[test]
fn test_gs026_kill_restart_resume_session() {
    let (db_path, _dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();

    // First daemon instance: create session + input
    let mut child1 = Command::new(daemon_bin())
        .args(["serve", "--db", &db])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon 1");

    let mut stdin1 = child1.stdin.take().expect("stdin1");
    let mut reader1 = BufReader::new(child1.stdout.take().expect("stdout1"));

    let create_resp = send_and_recv(
        &mut stdin1,
        &mut reader1,
        r#"{"request_id":"c1","method":"session.create","params":{"workspace":"/crash-test"},"actor":{"kind":"user"},"submitted_at":1000}"#,
    );
    assert_eq!(create_resp["success"], true);
    let sid = create_resp["events"][0]["payload"]["session_id"]
        .as_str()
        .expect("session_id")
        .to_string();

    let input_cmd = format!(
        r#"{{"request_id":"c2","method":"session.input","params":{{"content":"test crash recovery"}},"actor":{{"kind":"user"}},"session_id":"{sid}","submitted_at":1001}}"#
    );
    let input_resp = send_and_recv(&mut stdin1, &mut reader1, &input_cmd);
    assert_eq!(input_resp["success"], true);
    let last_seq = input_resp["latest_seq"].as_u64().unwrap();

    // Simulate crash: kill daemon without clean shutdown
    drop(stdin1);
    drop(reader1);
    child1.kill().expect("kill daemon 1");
    let _ = child1.wait();

    // Second daemon instance: restart with same DB
    let mut child2 = Command::new(daemon_bin())
        .args(["serve", "--db", &db])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon 2");

    let mut stdin2 = child2.stdin.take().expect("stdin2");
    let mut reader2 = BufReader::new(child2.stdout.take().expect("stdout2"));

    // Verify session recovered: snapshot should show active session with last_seq preserved
    let snap_cmd = format!(
        r#"{{"request_id":"c3","method":"session.snapshot","params":{{}},"actor":{{"kind":"user"}},"session_id":"{sid}","submitted_at":2000}}"#
    );
    let snap_resp = send_and_recv(&mut stdin2, &mut reader2, &snap_cmd);
    assert_eq!(snap_resp["success"], true, "snapshot failed: {snap_resp:?}");
    assert_eq!(snap_resp["result"]["session_id"], sid);
    assert_eq!(snap_resp["result"]["status"], "active");
    assert_eq!(snap_resp["result"]["last_seq"], last_seq);

    // Verify can continue session: send new input
    let resume_cmd = format!(
        r#"{{"request_id":"c4","method":"session.input","params":{{"content":"resumed after crash"}},"actor":{{"kind":"user"}},"session_id":"{sid}","submitted_at":2001}}"#
    );
    let resume_resp = send_and_recv(&mut stdin2, &mut reader2, &resume_cmd);
    assert_eq!(resume_resp["success"], true);
    assert_eq!(
        resume_resp["events"][0]["event_type"],
        "session.input_received"
    );
    let new_seq = resume_resp["latest_seq"].as_u64().unwrap();
    assert!(new_seq > last_seq, "seq should continue from {last_seq}");

    drop(stdin2);
    let status2 = child2.wait().expect("wait daemon 2");
    assert!(status2.success());
}

#[test]
fn test_stdio_malformed_json_returns_error() {
    let (db_path, _dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    // Malformed JSON
    let resp = send_and_recv(&mut stdin, &mut reader, r#"{not valid json}"#);
    assert_eq!(resp["success"], false, "malformed JSON should error");
    assert_eq!(resp["request_id"], "invalid-json");

    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success());
}

#[test]
fn test_stdio_event_replay_returns_ordered_events() {
    let (db_path, _dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    // Create session + 3 inputs
    let resp = send_and_recv(
        &mut stdin,
        &mut reader,
        r#"{"request_id":"r1","method":"session.create","params":{"workspace":"/tmp/ws"},"actor":{"kind":"user"},"submitted_at":2000}"#,
    );
    let sid = resp["events"][0]["payload"]["session_id"]
        .as_str()
        .expect("session_id")
        .to_string();

    for i in 1..=3 {
        let input = format!(
            r#"{{"request_id":"r{}","method":"session.input","params":{{"content":"msg{}"}},"actor":{{"kind":"user"}},"session_id":"{sid}","submitted_at":{}}}"#,
            1 + i,
            i,
            2000 + i
        );
        send_and_recv(&mut stdin, &mut reader, &input);
    }

    // event.replay from_seq=1 (exclusive: seq > 1 → seq=2,3,4)
    let replay_json = format!(
        r#"{{"request_id":"r5","method":"event.replay","params":{{"from_seq":1,"limit":10}},"actor":{{"kind":"user"}},"session_id":"{sid}","submitted_at":2005}}"#
    );
    let resp = send_and_recv(&mut stdin, &mut reader, &replay_json);
    assert_eq!(resp["success"], true, "event.replay failed: {resp:?}");
    let events = resp["events"].as_array().expect("events array");
    assert_eq!(events.len(), 3, "should replay 3 events from seq>1");
    assert_eq!(events[0]["seq"], 2);
    assert_eq!(events[0]["event_type"], "session.input_received");
    assert_eq!(events[1]["seq"], 3);
    assert_eq!(events[2]["seq"], 4);

    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success());
}

#[test]
fn test_stdio_empty_lines_ignored() {
    let (db_path, _dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    // Send an empty line first (should be ignored, no output)
    writeln!(stdin).expect("write empty line");
    stdin.flush().expect("flush");

    // Now send a valid command
    let resp = send_and_recv(
        &mut stdin,
        &mut reader,
        r#"{"request_id":"r1","method":"session.create","params":{"workspace":"/tmp/ws"},"actor":{"kind":"user"},"submitted_at":3000}"#,
    );
    assert_eq!(resp["success"], true);

    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success());
}

#[test]
fn test_stdio_runtime_config_external_llm_and_sidecar() {
    let (db_path, dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();
    let llm_script = write_llm_script(dir.path());
    let sidecar_script = write_sidecar_script(dir.path());
    let config_path = dir.path().join("runtime.json");
    let config = serde_json::json!({
        "llm_providers": [{
            "provider_id": "external",
            "program": powershell_program(),
            "args": ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", llm_script],
            "models": ["external/model"],
            "timeout_ms": 5000
        }],
        "sidecars": [{
            "tool_name": "external_tool",
            "program": powershell_program(),
            "args": ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", sidecar_script]
        }]
    });
    std::fs::write(&config_path, serde_json::to_string(&config).unwrap()).unwrap();
    let config = config_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db, "--runtime-config", &config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    let resp = send_and_recv(
        &mut stdin,
        &mut reader,
        r#"{"request_id":"cfg-1","method":"session.create","params":{"workspace":"/tmp/ws"},"actor":{"kind":"user"},"submitted_at":4000}"#,
    );
    assert_eq!(resp["success"], true, "session.create failed: {resp:?}");
    let session_id = resp["events"][0]["payload"]["session_id"]
        .as_str()
        .expect("session_id")
        .to_string();

    let llm = format!(
        r#"{{"request_id":"cfg-2","method":"llm.call","params":{{"llm_call_id":"llm-daemon","model":"external/model","provider":"external","prompt":"hello","auth_context":{{"type":"ApiKey","provider":"external","key":"sk-daemon-secret"}}}},"actor":{{"kind":"user"}},"session_id":"{session_id}","submitted_at":4001}}"#
    );
    let resp = send_and_recv(&mut stdin, &mut reader, &llm);
    assert_eq!(resp["success"], true, "llm.call failed: {resp:?}");
    assert_eq!(resp["events"][0]["event_type"], "llm.call_started");
    assert_eq!(resp["events"][1]["event_type"], "llm.call_finished");
    assert_eq!(
        resp["result"]["realtime_events"][0]["payload"]["text"],
        "daemon external text"
    );

    let tool = format!(
        r#"{{"request_id":"cfg-3","method":"tool.call","params":{{"tool_call_id":"tc-daemon","tool_name":"external_tool","tool_type":"sidecar","args":{{"value":9}}}},"actor":{{"kind":"user"}},"session_id":"{session_id}","submitted_at":4002}}"#
    );
    let resp = send_and_recv(&mut stdin, &mut reader, &tool);
    assert_eq!(resp["success"], true, "tool.call failed: {resp:?}");
    assert_eq!(resp["events"][1]["event_type"], "resource.sidecar_started");
    assert_eq!(
        resp["events"][3]["payload"]["result"],
        serde_json::json!({"external": true, "value": 9})
    );

    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success());
}

#[test]
fn test_stdio_leader_run_dispatches_native_tool() {
    let (db_path, dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();
    let observed_file = dir.path().join("leader-observed.txt");
    std::fs::write(&observed_file, "daemon leader observed content").unwrap();
    let llm_script = write_leader_tool_llm_script(dir.path(), &observed_file);
    let config_path = dir.path().join("leader_runtime.json");
    let config = serde_json::json!({
        "llm_providers": [{
            "provider_id": "external",
            "program": powershell_program(),
            "args": ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", llm_script],
            "models": ["external/model"],
            "timeout_ms": 5000
        }]
    });
    std::fs::write(&config_path, serde_json::to_string(&config).unwrap()).unwrap();
    let config = config_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db, "--runtime-config", &config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    let create_cmd = serde_json::json!({
        "request_id": "leader-1",
        "method": "session.create",
        "params": {"workspace": dir.path().display().to_string()},
        "actor": {"kind": "user"},
        "submitted_at": 7000
    });
    let create = send_and_recv(&mut stdin, &mut reader, &create_cmd.to_string());
    assert_eq!(create["success"], true, "session.create failed: {create:?}");
    let session_id = create["events"][0]["payload"]["session_id"]
        .as_str()
        .expect("session_id")
        .to_string();

    let cmd = serde_json::json!({
        "request_id": "leader-2",
        "method": "leader.run",
        "params": {
            "objective": "read the verification file and finish",
            "model": "external/model",
            "provider": "external",
            "max_rounds": 3
        },
        "actor": {"kind": "user"},
        "session_id": session_id,
        "submitted_at": 7001
    });
    let resp = send_and_recv(&mut stdin, &mut reader, &cmd.to_string());
    assert_eq!(resp["success"], true, "leader.run failed: {resp:?}");
    assert_eq!(resp["result"]["status"], "completed");
    assert_eq!(resp["result"]["answer"], "daemon leader final");
    assert_eq!(
        resp["result"]["observations"][0]["result"]["content"],
        "daemon leader observed content"
    );
    let event_types: Vec<_> = resp["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["event_type"].as_str().unwrap())
        .collect();
    assert!(event_types.contains(&"tool.call_completed"));

    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success());
}

#[test]
fn test_stdio_user_task_completes_end_to_end() {
    let (db_path, dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();
    let llm_script = write_task_llm_script(dir.path());
    let config_path = dir.path().join("task_runtime.json");
    let config = serde_json::json!({
        "llm_providers": [{
            "provider_id": "external",
            "program": powershell_program(),
            "args": ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", llm_script],
            "models": ["external/model"],
            "timeout_ms": 5000
        }]
    });
    std::fs::write(&config_path, serde_json::to_string(&config).unwrap()).unwrap();
    let config = config_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db, "--runtime-config", &config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    let resp = send_and_recv(
        &mut stdin,
        &mut reader,
        r#"{"request_id":"task-1","method":"session.run_task","params":{"content":"write a concise migration summary","task_id":"task-user-input-1","workspace":"/tmp/ws","model":"external/model","provider":"external","auth_context":{"type":"ApiKey","provider":"external","key":"sk-task-e2e"}},"actor":{"kind":"user"},"submitted_at":5000}"#,
    );
    assert_eq!(resp["success"], true, "session.run_task failed: {resp:?}");
    assert_eq!(resp["result"]["status"], "completed");
    assert_eq!(
        resp["result"]["answer"],
        "TASK_DONE: write a concise migration summary"
    );
    let event_types: Vec<_> = resp["events"]
        .as_array()
        .expect("events")
        .iter()
        .map(|event| event["event_type"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "session.created",
            "session.input_received",
            "task.created",
            "task.assigned",
            "llm.call_started",
            "llm.call_finished",
            "task.completed",
            "session.completed",
        ]
    );
    let session_id = resp["result"]["session_id"].as_str().unwrap().to_string();
    let task_id = resp["result"]["task_id"].as_str().unwrap().to_string();

    let snapshot_json = format!(
        r#"{{"request_id":"task-2","method":"session.snapshot","params":{{}},"actor":{{"kind":"user"}},"session_id":"{session_id}","submitted_at":5001}}"#
    );
    let snapshot = send_and_recv(&mut stdin, &mut reader, &snapshot_json);
    assert_eq!(snapshot["result"]["status"], "completed");

    let task_list_json = format!(
        r#"{{"request_id":"task-3","method":"task.list","params":{{}},"actor":{{"kind":"user"}},"session_id":"{session_id}","submitted_at":5002}}"#
    );
    let tasks = send_and_recv(&mut stdin, &mut reader, &task_list_json);
    let task = tasks["result"]["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["task_id"] == task_id)
        .expect("completed task present");
    assert_eq!(task["status"], "terminal");

    let replay_json = format!(
        r#"{{"request_id":"task-4","method":"event.replay","params":{{"from_seq":0,"limit":100}},"actor":{{"kind":"user"}},"session_id":"{session_id}","submitted_at":5003}}"#
    );
    let replay = send_and_recv(&mut stdin, &mut reader, &replay_json);
    let replay_events = replay["events"].as_array().unwrap();
    assert_eq!(
        replay_events.last().unwrap()["event_type"],
        "session.completed"
    );
    assert!(replay_events
        .iter()
        .all(|event| !event["event_type"].as_str().unwrap().contains("_delta")));
    let replay_count = replay_events.len();

    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success());

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db, "--runtime-config", &config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("restart daemon serve");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    let list = send_and_recv(
        &mut stdin,
        &mut reader,
        r#"{"request_id":"task-5","method":"session.list","params":{},"actor":{"kind":"user"},"submitted_at":5004}"#,
    );
    let restarted = list["result"]["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|session| session["session_id"] == session_id)
        .expect("session survives restart");
    assert_eq!(restarted["status"], "completed");

    let replay = send_and_recv(&mut stdin, &mut reader, &replay_json);
    assert_eq!(replay["events"].as_array().unwrap().len(), replay_count);

    drop(stdin);
    let status = child.wait().expect("wait restarted daemon");
    assert!(status.success());
}

#[test]
fn test_stdio_user_task_completes_with_real_openai_provider_when_key_is_present() {
    let Ok(api_key) = std::env::var("OPENAI_API_KEY") else {
        eprintln!("skipping real OpenAI E2E: OPENAI_API_KEY is not set");
        return;
    };
    if api_key.trim().is_empty() {
        eprintln!("skipping real OpenAI E2E: OPENAI_API_KEY is empty");
        return;
    }

    let (db_path, dir) = make_db_path();
    let db = db_path.to_string_lossy().to_string();
    let provider = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(if cfg!(windows) {
            "lingxiao-llm-openai-provider.exe"
        } else {
            "lingxiao-llm-openai-provider"
        });
    if !provider.exists() {
        eprintln!(
            "skipping real OpenAI E2E: provider binary not found at {}",
            provider.display()
        );
        return;
    }

    let model = std::env::var("OPENAI_MODEL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "gpt-4o-mini".to_string());
    let base_url = std::env::var("OPENAI_BASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
    let api_kind = std::env::var("OPENAI_API_KIND")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "chat".to_string());

    let config_path = dir.path().join("real_openai_runtime.json");
    let config = serde_json::json!({
        "llm_providers": [{
            "provider_id": "openai",
            "program": provider,
            "args": [],
            "models": [model],
            "timeout_ms": 120000
        }]
    });
    std::fs::write(&config_path, serde_json::to_string(&config).unwrap()).unwrap();
    let config = config_path.to_string_lossy().to_string();

    let mut child = Command::new(daemon_bin())
        .args(["serve", "--db", &db, "--runtime-config", &config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon serve");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    let cmd = serde_json::json!({
        "request_id": "real-openai-1",
        "method": "session.run_task",
        "params": {
            "content": "Reply with exactly: TASK_DONE_REAL_PROVIDER",
            "task_id": "real-openai-task",
            "workspace": db,
            "model": model,
            "provider": "openai",
            "auth_context": {
                "type": "ApiKey",
                "provider": "openai",
                "key": api_key
            },
            "options": {
                "max_tokens": 16,
                "temperature": 0.0,
                "metadata": {
                    "base_url": base_url,
                    "api": api_kind
                }
            }
        },
        "actor": {"kind": "user"},
        "submitted_at": 6000
    });
    let resp = send_and_recv(&mut stdin, &mut reader, &cmd.to_string());
    assert_eq!(resp["success"], true, "session.run_task failed: {resp:?}");
    assert_eq!(resp["result"]["status"], "completed");
    assert!(
        resp["result"]["answer"]
            .as_str()
            .unwrap_or_default()
            .contains("TASK_DONE_REAL_PROVIDER"),
        "unexpected real provider answer: {}",
        resp["result"]["answer"]
    );
    assert_eq!(
        resp["events"].as_array().unwrap().last().unwrap()["event_type"],
        "session.completed"
    );

    drop(stdin);
    let status = child.wait().expect("wait daemon");
    assert!(status.success());
}

fn write_llm_script(dir: &std::path::Path) -> PathBuf {
    let script = dir.join("llm_provider.ps1");
    std::fs::write(
        &script,
        r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
if ($req.auth_context.key -ne 'sk-daemon-secret') { exit 2 }
@{ TextDelta = 'daemon external text' } | ConvertTo-Json -Compress
@{ Usage = @{
    prompt_tokens = 1
    completion_tokens = 2
    total_tokens = 3
    cache_creation_input_tokens = $null
    cache_read_input_tokens = $null
    reasoning_tokens = $null
  }
} | ConvertTo-Json -Depth 8 -Compress
@{ Finished = 'Stop' } | ConvertTo-Json -Compress
"#,
    )
    .unwrap();
    script
}

fn write_task_llm_script(dir: &std::path::Path) -> PathBuf {
    let script = dir.join("task_llm_provider.ps1");
    std::fs::write(
        &script,
        r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
if ($req.auth_context.key -ne 'sk-task-e2e') { exit 2 }
@{ ThinkingDelta = 'planning the task' } | ConvertTo-Json -Compress
@{ TextDelta = "TASK_DONE: $($req.messages[0].content)" } | ConvertTo-Json -Compress
@{ Usage = @{
    prompt_tokens = 7
    completion_tokens = 11
    total_tokens = 18
    cache_creation_input_tokens = $null
    cache_read_input_tokens = $null
    reasoning_tokens = 2
  }
} | ConvertTo-Json -Depth 8 -Compress
@{ Finished = 'Stop' } | ConvertTo-Json -Compress
"#,
    )
    .unwrap();
    script
}

fn write_leader_tool_llm_script(dir: &std::path::Path, target: &std::path::Path) -> PathBuf {
    let script = dir.join("leader_tool_llm_provider.ps1");
    let target = target.to_string_lossy().replace('\'', "''");
    std::fs::write(
        &script,
        format!(
            r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
$hasToolObservation = $false
foreach ($message in $req.messages) {{
  if ($message.role -eq 'tool') {{
    $hasToolObservation = $true
  }}
}}
if (-not $hasToolObservation) {{
  @{{
    ToolCall = @{{
      id = 'daemon-leader-read'
      name = 'file_read'
      arguments = @{{
        path = '{target}'
      }}
    }}
  }} | ConvertTo-Json -Depth 8 -Compress
  @{{ Finished = 'ToolCalls' }} | ConvertTo-Json -Compress
}} else {{
  @{{ TextDelta = 'daemon leader final' }} | ConvertTo-Json -Compress
  @{{ Finished = 'Stop' }} | ConvertTo-Json -Compress
}}
"#
        ),
    )
    .unwrap();
    script
}

fn write_sidecar_script(dir: &std::path::Path) -> PathBuf {
    let script = dir.join("sidecar_provider.ps1");
    std::fs::write(
        &script,
        r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
$args = [System.Text.Encoding]::UTF8.GetString([byte[]]$req.args) | ConvertFrom-Json
$payload = "{""external"":true,""value"":$($args.value)}"
$response = @{
  Completed = @{
    request_id = $req.request_id
    result = [System.Text.Encoding]::UTF8.GetBytes($payload)
    result_shape = 'json'
    duration_ms = 1
    usage = @{
      runtime_ms = 1
      cpu_ms = 1
      memory_mb_peak = 1
      network_bytes = 0
      file_write_bytes = 0
    }
  }
}
$response | ConvertTo-Json -Depth 8 -Compress
"#,
    )
    .unwrap();
    script
}

/// Resolve an absolute `powershell.exe` path. Rust's `Command` on Windows does
/// not perform PATHEXT resolution, so a bare `"powershell"` embedded in runtime
/// config can fail to spawn. Anchor to the system root when available.
fn powershell_program() -> String {
    for key in ["SystemRoot", "windir", "SYSTEMROOT", "WINDIR"] {
        if let Ok(root) = std::env::var(key) {
            let candidate = std::path::Path::new(&root)
                .join("System32")
                .join("WindowsPowerShell")
                .join("v1.0")
                .join("powershell.exe");
            if candidate.exists() {
                return candidate.to_string_lossy().to_string();
            }
        }
    }
    "powershell.exe".to_string()
}
