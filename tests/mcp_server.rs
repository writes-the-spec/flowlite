//! `flowlite mcp` — the handshake, over the same pipes an MCP client would use.
//!
//! Driven through the built binary rather than as unit tests because the thing under test
//! is the process: that the server reaches stdin and stdout at all, that *nothing else in
//! the binary writes a byte to stdout* and corrupts the framing, and that closing stdin
//! ends it. None of the three is observable from inside the library.
//!
//! MCP over stdio is newline-delimited JSON-RPC 2.0: one JSON object per line in, one per
//! line out. This cut's four read tools are exercised here too, rather than in a file of
//! their own, because the property under test is the same one the handshake tests are:
//! that a real client, over real pipes, gets back what the tool promises.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use serde_json::{json, Value};

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard, BINARY};

/// Generous enough that a cold first connection - which migrates both schemas - is never
/// mistaken for a server that has stopped answering.
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

fn data_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-mcp-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

/// A client speaking to `flowlite mcp` over real pipes.
///
/// stdout is read on its own thread so that every read here has a deadline: a blocking
/// read against a server that answered nothing would otherwise hang the whole suite
/// rather than fail this test.
struct McpClient {
    server: ServerGuard,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    next_id: i64,
}

impl McpClient {

    fn start(dir: &Path) -> Self {
        let mut child = Command::new(BINARY)
            .args(["--data-dir", &dir.to_string_lossy(), "mcp"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let (sender, lines) = mpsc::channel();

        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };

                if sender.send(line).is_err() {
                    return;
                }
            }
        });

        McpClient {
            server: ServerGuard::new(child, libc::SIGTERM),
            stdin: Some(stdin),
            lines,
            next_id: 1,
        }
    }

    fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().expect("stdin was already closed");

        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    fn read_line(&mut self) -> String {
        match self.lines.recv_timeout(REPLY_TIMEOUT) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => panic!("the server sent nothing in {REPLY_TIMEOUT:?}"),
            Err(RecvTimeoutError::Disconnected) => {
                panic!("the server closed stdout: {}", self.server.take_stderr())
            }
        }
    }

    /// Sends a request and returns the `result` of its response, failing on a JSON-RPC
    /// error - which for these cases is never the expected answer.
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;

        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }));

        let line = self.read_line();

        let response: Value = serde_json::from_str(&line)
            .unwrap_or_else(|e| panic!("stdout was not one JSON object per line: {e}: {line}"));

        assert_eq!(response["id"], json!(id), "answered a different request: {line}");
        assert!(response["error"].is_null(), "{method} failed: {line}");

        response["result"].clone()
    }

    fn notify(&mut self, method: &str) {
        self.send(json!({ "jsonrpc": "2.0", "method": method }));
    }

    fn initialize(&mut self) -> Value {
        self.request("initialize", json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "flowlite-tests", "version": "0" },
        }))
    }

    /// The handshake every test below needs before it can call a tool: `initialize`, then
    /// the notification that ends it. Neither response is used by the caller.
    fn handshake(&mut self) {
        self.initialize();
        self.notify("notifications/initialized");
    }

    /// Calls a tool and returns its `CallToolResult` - a JSON-RPC *success*, per the
    /// design's "errors are tool results, not protocol errors": `request` already asserts
    /// there is no protocol-level `error`, so a domain failure is still reached through
    /// this method, distinguished by `result["isError"]`.
    fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        self.request("tools/call", json!({ "name": name, "arguments": arguments }))
    }

    /// Closes stdin, which is how an MCP client stops a server it spawned, and reports how
    /// the process ended.
    fn close_stdin(&mut self) {
        self.stdin.take();
    }
}

/// The text content of a tool result - what every MCP client can read, per the design's
/// rule that structured content rides alongside it rather than replacing it.
fn tool_text(result: &Value) -> &str {
    result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text content in {result}"))
}

/// The first thing any client does, and the answer a client shows the user when it lists
/// its servers - so the name is the one in the install line, not the crate's default.
#[test]
fn initialize_names_the_server_and_its_version() {
    let dir = data_dir("initialize");
    let mut client = McpClient::start(&dir);

    let result = client.initialize();

    assert_eq!(result["serverInfo"]["name"], json!("flowlite"));
    assert_eq!(result["serverInfo"]["version"], json!(env!("CARGO_PKG_VERSION")));
}

/// The capability is what makes a client ask for tools at all - the durable half of this
/// test, true whether zero tools are registered or four. The other half, replaced from cut
/// 2's "and lists none yet", is this cut's own: the four read tools, and nothing else,
/// named by `tools/list`.
#[test]
fn the_handshake_declares_tools_and_lists_the_four_read_tools() {
    let dir = data_dir("tools");
    let mut client = McpClient::start(&dir);

    let initialized = client.initialize();
    assert!(
        initialized["capabilities"]["tools"].is_object(),
        "the tools capability was not declared: {initialized}",
    );

    client.notify("notifications/initialized");

    let listed = client.request("tools/list", json!({}));

    let mut names: Vec<&str> = listed["tools"]
        .as_array()
        .expect("tools/list did not return an array")
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    names.sort();

    assert_eq!(names, vec!["get_job_run", "get_task_output", "list_job_runs", "list_jobs"]);
}

/// The only way a client stops a server it spawned. A process that lingered would outlive
/// every agent session that ever opened it.
#[test]
fn closing_stdin_ends_the_process_cleanly() {
    let dir = data_dir("stdin");
    let mut client = McpClient::start(&dir);

    client.initialize();
    client.notify("notifications/initialized");

    client.close_stdin();

    let status = client.server.wait_for_exit(Duration::from_secs(30));

    let status = status.expect("the server was still running after its stdin was closed");
    assert!(status.success(), "the server exited with {status}");
}

/// The projection `list_jobs` exists for: a job installed in the data directory is exactly
/// what `job list --json` would print, read back through MCP instead of the CLI.
#[test]
fn list_jobs_shows_a_job_installed_in_the_data_directory() {
    let dir = data_dir("list-jobs");
    install_job(&dir, "hello.yaml", "id: hello\nname: Hello\ntasks:\n  - id: say\n    command: \"true\"\n");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("list_jobs", json!({}));
    assert_ne!(result["isError"], json!(true), "{result}");

    let jobs: Value = serde_json::from_str(tool_text(&result)).unwrap();
    let job_ids: Vec<&str> = jobs.as_array().unwrap().iter().map(|job| job["job_id"].as_str().unwrap()).collect();

    assert_eq!(job_ids, vec!["hello"]);
}

/// The staleness half of the fresh-mem mechanism: `mem` is seeded once per call, not once
/// per process, so a file written into `jobs/` after this long-lived server started is
/// still visible on the very next call - the whole reason a CLI command's "read config,
/// then exit" cannot simply be ported unchanged into a server.
#[test]
fn a_job_file_written_after_the_server_started_is_visible_to_list_jobs() {
    let dir = data_dir("late-job");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let before: Value = serde_json::from_str(tool_text(&client.call_tool("list_jobs", json!({})))).unwrap();
    assert_eq!(before.as_array().unwrap().len(), 0, "{before}");

    install_job(&dir, "late.yaml", "id: late\nname: Late\ntasks:\n  - id: say\n    command: \"true\"\n");

    let after: Value = serde_json::from_str(tool_text(&client.call_tool("list_jobs", json!({})))).unwrap();
    let job_ids: Vec<&str> = after.as_array().unwrap().iter().map(|job| job["job_id"].as_str().unwrap()).collect();

    assert_eq!(job_ids, vec!["late"], "the file written after startup was not picked up: {after}");
}

/// `anyhow::bail!("Job run {} not found", ...)` is the sentence `job-run get` already
/// raises; the tool error carries it verbatim rather than a protocol error the model
/// cannot read.
#[test]
fn get_job_run_on_an_unknown_id_is_a_tool_error_naming_the_id() {
    let dir = data_dir("unknown-run");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("get_job_run", json!({ "job_run_id": 404 }));

    assert_eq!(result["isError"], json!(true), "{result}");
    assert!(tool_text(&result).contains("404"), "{result}");
}

/// End to end: a real run, executed by a real `serve` process, read back through MCP - the
/// same task output `job-run logs --json` would print, since there is no `submit_job` tool
/// yet to reach it any other way.
#[test]
fn get_task_output_returns_what_a_tasks_command_echoed() {
    let dir = data_dir("task-output");
    install_job(&dir, "echoer.yaml", "id: echoer\nname: Echoer\ntasks:\n  - id: say\n    command: echo mcp-task-output\n");

    let mut server = ServerGuard::new(serve(&dir, 18230), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let submitted = flowlite(&dir, &["job", "submit", "echoer", "--wait", "--json"]);
    assert!(submitted.status.success(), "{}", String::from_utf8_lossy(&submitted.stderr));

    let run: Value = serde_json::from_slice(&submitted.stdout).unwrap();
    let job_run_id = run["id"].as_i64().unwrap();

    server.stop();

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("get_task_output", json!({ "job_run_id": job_run_id }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let logs: Value = serde_json::from_str(tool_text(&result)).unwrap();
    let stdout = logs[0]["stdout"].as_str().unwrap();

    assert!(stdout.contains("mcp-task-output"), "{stdout}");
}

/// `max_bytes` bends the "identical to `--json`" rule only in length, by an amount it
/// states: a stream longer than the limit comes back carrying the truncation marker this
/// cut's unit tests pin the exact wording of. The task writes to both streams so this
/// exercises the two independent budgets end to end, not stdout alone.
#[test]
fn a_long_stream_comes_back_truncated_carrying_the_marker() {
    let dir = data_dir("truncated");
    install_job(
        &dir,
        "verbose.yaml",
        "id: verbose\nname: Verbose\ntasks:\n  - id: say\n    command: \"echo 0123456789; echo abcdefghij 1>&2\"\n",
    );

    let mut server = ServerGuard::new(serve(&dir, 18231), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let submitted = flowlite(&dir, &["job", "submit", "verbose", "--wait", "--json"]);
    assert!(submitted.status.success(), "{}", String::from_utf8_lossy(&submitted.stderr));

    let run: Value = serde_json::from_slice(&submitted.stdout).unwrap();
    let job_run_id = run["id"].as_i64().unwrap();

    server.stop();

    let mut client = McpClient::start(&dir);
    client.handshake();

    // Each echo writes 11 bytes (10 characters plus the newline); keeping 4 keeps only
    // the last 4 of each stream, independently.
    let result = client.call_tool("get_task_output", json!({ "job_run_id": job_run_id, "max_bytes": 4 }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let logs: Value = serde_json::from_str(tool_text(&result)).unwrap();
    let stdout = logs[0]["stdout"].as_str().unwrap();
    let stderr = logs[0]["stderr"].as_str().unwrap();

    assert!(stdout.starts_with("[truncated: "), "{stdout}");
    assert!(stdout.contains("earlier bytes dropped]"), "{stdout}");
    assert!(stdout.ends_with("789\n"), "{stdout}");

    assert!(stderr.starts_with("[truncated: "), "{stderr}");
    assert!(stderr.contains("earlier bytes dropped]"), "{stderr}");
    assert!(stderr.ends_with("hij\n"), "{stderr}");
}
