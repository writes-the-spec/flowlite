//! `flowlite mcp` — the handshake, over the same pipes an MCP client would use.
//!
//! Driven through the built binary rather than as unit tests because the thing under test
//! is the process: that the server reaches stdin and stdout at all, that *nothing else in
//! the binary writes a byte to stdout* and corrupts the framing, and that closing stdin
//! ends it. None of the three is observable from inside the library.
//!
//! MCP over stdio is newline-delimited JSON-RPC 2.0: one JSON object per line in, one per
//! line out. The four read tools and this cut's `submit_job` are exercised here too,
//! rather than in a file of their own, because the property under test is the same one the
//! handshake tests are: that a real client, over real pipes, gets back what the tool
//! promises.

use std::collections::HashMap;
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
    /// A response read while waiting for a different id, kept here until the caller that
    /// id belongs to asks for it. Needed once two requests are ever sent before either
    /// response is read: rmcp dispatches each `tools/call` to its own task
    /// (src/mcp/mod.rs), so responses to overlapping requests can come back in either
    /// order, not the order the requests were sent in.
    buffered_responses: HashMap<i64, Value>,
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
            buffered_responses: HashMap::new(),
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

    /// Sends a request without waiting for its response, and returns the id to read it back
    /// with later. This is what lets a second request be sent while the first is still in
    /// flight, so two `tools/call`s can genuinely overlap inside the server rather than the
    /// second only starting once the first has already returned and dropped its
    /// connections - see `receive_response`.
    fn send_request(&mut self, method: &str, params: Value) -> i64 {
        let id = self.next_id;
        self.next_id += 1;

        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }));

        id
    }

    /// Reads lines until the response for `id` arrives, buffering any other response that
    /// arrives first. Two requests sent before either is read can answer in either order -
    /// rmcp dispatches each to its own task - so this cannot assume the next line on the
    /// wire is the one this call is waiting for.
    fn receive_response(&mut self, id: i64) -> Value {
        let response = match self.buffered_responses.remove(&id) {
            Some(response) => response,
            None => loop {
                let line = self.read_line();

                let response: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|e| panic!("stdout was not one JSON object per line: {e}: {line}"));

                let response_id = response["id"].as_i64()
                    .unwrap_or_else(|| panic!("response carried no integer id: {line}"));

                if response_id == id {
                    break response;
                }

                self.buffered_responses.insert(response_id, response);
            },
        };

        assert!(response["error"].is_null(), "request {id} failed: {response}");

        response["result"].clone()
    }

    /// Sends a request and returns the `result` of its response, failing on a JSON-RPC
    /// error - which for these cases is never the expected answer. Sequential convenience
    /// over `send_request` + `receive_response`, for every call that does not need the two
    /// separated.
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.send_request(method, params);
        self.receive_response(id)
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

    /// Sends a `tools/call` without waiting for its response - see `send_request`.
    fn send_call_tool(&mut self, name: &str, arguments: Value) -> i64 {
        self.send_request("tools/call", json!({ "name": name, "arguments": arguments }))
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
/// test, true whether zero tools are registered or six. The other half, extended cut by
/// cut, is this cut's own: `stop_job_run` alongside the other five, and nothing else,
/// named by `tools/list` - the full set the design promises.
#[test]
fn the_handshake_declares_tools_and_lists_the_six_tools() {
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

    assert_eq!(
        names,
        vec!["get_job_run", "get_task_output", "list_job_runs", "list_jobs", "stop_job_run", "submit_job"],
    );
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

const HELLO: &str = "id: hello\nname: Hello\ntasks:\n  - id: say\n    command: \"true\"\n";

/// A job that outlives every short `wait_seconds` this file waits with, so a wait can be
/// observed elapsing, and a stop can be observed reaching a run that is genuinely running
/// rather than one the dispatcher settled before it ever started.
const SLEEPER: &str = "id: sleeper\nname: Sleeper\ntasks:\n  - id: say\n    command: \"sleep 30\"\n";

/// The `job` argument: an installed job submits without touching the filesystem, and the
/// result is the pending run `job submit --json` would have printed for it.
#[test]
fn submitting_an_installed_job_returns_its_pending_run() {
    let dir = data_dir("submit-installed");
    install_job(&dir, "hello.yaml", HELLO);

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({ "job": "hello" }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let job_run: Value = serde_json::from_str(tool_text(&result)).unwrap();
    assert!(job_run["id"].as_i64().unwrap() > 0, "{job_run}");
    assert_eq!(job_run["status"], json!("pending"), "{job_run}");
}

/// `yaml` is the natural agent action: nothing is written to disk, and the run's config
/// snapshot is what keeps it inspectable once submitted.
#[test]
fn submitting_an_inline_yaml_definition_succeeds() {
    let dir = data_dir("submit-inline");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({
        "yaml": "id: probe\nname: Probe\ntasks:\n  - id: say\n    command: \"true\"\n",
    }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let job_run: Value = serde_json::from_str(tool_text(&result)).unwrap();
    assert_eq!(job_run["job_id"], json!("probe"), "{job_run}");
    assert_eq!(job_run["status"], json!("pending"), "{job_run}");
}

/// The regression test for the whole fresh-mem mechanism this cut exists for: two calls
/// seeding the same inline id must not collide, because each seeds a `mem` nothing else has
/// the name of.
///
/// Both `tools/call` requests are sent before either response is read, so they genuinely
/// overlap inside the server rather than running one after the other: rmcp dispatches each
/// to its own task (src/mcp/mod.rs's own doc comment says calls run concurrently), so two
/// calls in flight at once is the real shape of the hazard `with_fresh_mem` exists to
/// prevent. A version of this test that waited for the first response before sending the
/// second would not catch a regression here: `mem` is a shared-cache database that SQLite
/// drops the instant nothing has it open (`src/toolkit.rs`), so a fully sequential first
/// call's rows are already gone by the time a second, later call starts - there would be
/// nothing left to collide with, and the test would pass even with `with_fresh_mem` deleted.
/// (Confirmed by hand while fixing this: temporarily replacing `with_fresh_mem()` with a
/// plain `.clone()` in `submit_job_run` still passed the old sequential version of this
/// test, which is exactly why it is written this way now.)
///
/// Relies on `src/toolkit.rs`'s `MIGRATION_LOCK` to keep the two calls' own disk-schema
/// migrations (each call's `get_conn` runs one) from racing each other on the file both
/// calls share - a real hazard, discovered while building this test, but a separate one
/// from the `mem` collision this test exists to catch.
#[test]
fn two_concurrent_submits_of_the_same_inline_id_do_not_collide() {
    let dir = data_dir("submit-inline-concurrent");
    let yaml = "id: probe\nname: Probe\ntasks:\n  - id: say\n    command: \"true\"\n";

    let mut client = McpClient::start(&dir);
    client.handshake();

    let first_id = client.send_call_tool("submit_job", json!({ "yaml": yaml }));
    let second_id = client.send_call_tool("submit_job", json!({ "yaml": yaml }));

    let first = client.receive_response(first_id);
    let second = client.receive_response(second_id);

    assert_ne!(first["isError"], json!(true), "{first}");
    assert_ne!(second["isError"], json!(true), "{second}");

    let first_run: Value = serde_json::from_str(tool_text(&first)).unwrap();
    let second_run: Value = serde_json::from_str(tool_text(&second)).unwrap();

    assert_eq!(first_run["job_id"], json!("probe"), "{first_run}");
    assert_eq!(second_run["job_id"], json!("probe"), "{second_run}");
    assert_ne!(first_run["id"], second_run["id"], "two submits must get two different run ids");
}

/// `file` reads a definition where it lies and never installs it - the same guarantee
/// `job submit -f` gives, reached through the tool instead of the CLI.
#[test]
fn submitting_a_file_does_not_install_it() {
    let dir = data_dir("submit-file");
    let file = dir.join("outside.yaml");
    std::fs::write(&file, "id: outside\nname: Outside\ntasks:\n  - id: say\n    command: \"true\"\n").unwrap();

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({ "file": file.to_string_lossy() }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let job_run: Value = serde_json::from_str(tool_text(&result)).unwrap();
    assert_eq!(job_run["job_id"], json!("outside"), "{job_run}");

    let listed: Value = serde_json::from_str(tool_text(&client.call_tool("list_jobs", json!({})))).unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 0, "the file was installed: {listed}");
}

/// `job_id` is `mem.job`'s primary key, so the alternative to refusing is a raw constraint
/// error. The remedy names the shape this tool itself takes - not the CLI's `-f`, which
/// this caller never passed.
#[test]
fn an_inline_definition_colliding_with_an_installed_job_is_refused_by_name() {
    let dir = data_dir("submit-collision");
    install_job(&dir, "hello.yaml", HELLO);

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({ "yaml": HELLO }));

    assert_eq!(result["isError"], json!(true), "{result}");
    let text = tool_text(&result);
    assert!(text.contains("hello"), "{text}");
    assert!(text.contains(r#"{ "job": "hello" }"#), "{text}");
}

/// The text names the reason, exactly as `job submit -f` reports a parse failure - here, a
/// missing required field, `id`.
#[test]
fn invalid_inline_yaml_is_a_tool_error_naming_the_reason() {
    let dir = data_dir("submit-invalid");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({
        "yaml": "name: No Id\ntasks:\n  - id: say\n    command: \"true\"\n",
    }));

    assert_eq!(result["isError"], json!(true), "{result}");
    assert!(tool_text(&result).contains("id"), "{}", tool_text(&result));
}

/// `resolve_job_parameters` still refuses a name the job's YAML does not declare, reached
/// through the tool exactly as `--param` reaches it.
#[test]
fn a_param_the_job_does_not_declare_is_refused() {
    let dir = data_dir("submit-bad-param");
    install_job(&dir, "hello.yaml", HELLO);

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({
        "job": "hello",
        "params": { "region": "eu" },
    }));

    assert_eq!(result["isError"], json!(true), "{result}");
    assert!(tool_text(&result).contains("region"), "{}", tool_text(&result));
}

/// Queuing work for a server that is not up yet is legitimate, but an agent that got back
/// `pending` with no warning would poll a run that cannot start - so the JSON is followed
/// by a second content block naming the directory, and only while nothing is serving it.
#[test]
fn the_warning_block_appears_only_while_the_directory_is_unserved() {
    let dir = data_dir("submit-unserved");
    install_job(&dir, "hello.yaml", HELLO);

    let mut client = McpClient::start(&dir);
    client.handshake();

    let unserved = client.call_tool("submit_job", json!({ "job": "hello" }));
    assert_ne!(unserved["isError"], json!(true), "{unserved}");
    let unserved_content = unserved["content"].as_array().unwrap();
    assert_eq!(unserved_content.len(), 2, "{unserved}");
    assert!(
        unserved_content[1]["text"].as_str().unwrap().contains(&dir.to_string_lossy().to_string()),
        "{unserved}",
    );

    let mut server = ServerGuard::new(serve(&dir, 18232), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let served = client.call_tool("submit_job", json!({ "job": "hello" }));
    assert_ne!(served["isError"], json!(true), "{served}");
    let served_content = served["content"].as_array().unwrap();
    assert_eq!(served_content.len(), 1, "{served}");

    server.stop();
}

/// `get_job_run`'s `status` field, read without waiting - `until`'s predicate below needs
/// a plain boolean, and this is the one query it polls with.
fn job_run_status(client: &mut McpClient, job_run_id: i64) -> String {
    let result = client.call_tool("get_job_run", json!({ "job_run_id": job_run_id }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let detail: Value = serde_json::from_str(tool_text(&result)).unwrap();
    detail["status"].as_str().unwrap().to_string()
}

/// `wait_seconds` closes the loop in one turn: a job that succeeds comes back settled
/// rather than merely queued - the whole reason `submit_job` grew a wait at all.
#[test]
fn submitting_with_wait_seconds_against_a_served_directory_comes_back_settled() {
    let dir = data_dir("wait-submit-succeeds");
    install_job(&dir, "hello.yaml", HELLO);

    let mut server = ServerGuard::new(serve(&dir, 18233), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({ "job": "hello", "wait_seconds": 10 }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let job_run: Value = serde_json::from_str(tool_text(&result)).unwrap();
    assert_eq!(job_run["status"], json!("succeeded"), "{job_run}");

    server.stop();
}

/// A wait that elapses before the run settles is answered, not refused: the id and its
/// unfinished status are exactly what let the agent ask again, which a timeout error would
/// have thrown away along with the id.
#[test]
fn a_wait_that_expires_returns_the_run_still_running() {
    let dir = data_dir("wait-submit-expires");
    install_job(&dir, "sleeper.yaml", SLEEPER);

    let mut server = ServerGuard::new(serve(&dir, 18234), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({ "job": "sleeper", "wait_seconds": 1 }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let job_run: Value = serde_json::from_str(tool_text(&result)).unwrap();
    assert!(job_run["id"].as_i64().unwrap() > 0, "{job_run}");

    let status = job_run["status"].as_str().unwrap();
    assert!(
        status == "pending" || status == "running",
        "expected an unfinished status, got {status}: {job_run}",
    );

    server.stop();
}

/// `get_job_run`'s own wait: submitted without one, then read back through a second tool
/// call that waits instead - the same bound and the same clamp, shared through `wait.rs`
/// rather than reimplemented per tool.
#[test]
fn get_job_run_with_wait_seconds_returns_the_settled_run() {
    let dir = data_dir("wait-get-job-run");
    install_job(&dir, "hello.yaml", HELLO);

    let mut server = ServerGuard::new(serve(&dir, 18235), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let submitted = client.call_tool("submit_job", json!({ "job": "hello" }));
    let job_run_id = serde_json::from_str::<Value>(tool_text(&submitted)).unwrap()["id"].as_i64().unwrap();

    let result = client.call_tool("get_job_run", json!({ "job_run_id": job_run_id, "wait_seconds": 10 }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let detail: Value = serde_json::from_str(tool_text(&result)).unwrap();
    assert_eq!(detail["status"], json!("succeeded"), "{detail}");

    server.stop();
}

/// `stop_job_run` waited settles a run that is genuinely running, not one the dispatcher
/// merely skipped before it ever started - the case that exercises the orchestrator's own
/// stop handling.
#[test]
fn stop_job_run_with_wait_seconds_settles_a_running_run() {
    let dir = data_dir("wait-stop-running");
    install_job(&dir, "sleeper.yaml", SLEEPER);

    let mut server = ServerGuard::new(serve(&dir, 18236), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let submitted = client.call_tool("submit_job", json!({ "job": "sleeper" }));
    let job_run_id = serde_json::from_str::<Value>(tool_text(&submitted)).unwrap()["id"].as_i64().unwrap();

    assert!(
        until(Duration::from_secs(30), || job_run_status(&mut client, job_run_id) == "running"),
        "the run never started running",
    );

    let result = client.call_tool("stop_job_run", json!({ "job_run_id": job_run_id, "wait_seconds": 10 }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let job_run: Value = serde_json::from_str(tool_text(&result)).unwrap();
    let status = job_run["status"].as_str().unwrap();
    assert!(
        status != "pending" && status != "running",
        "expected a settled status, got {status}: {job_run}",
    );

    server.stop();
}

/// Without a wait, `stop_job_run` still returns the `JobRun` row rather than the CLI's
/// `{job_run_id, stop_requested}` shape, so a caller reads `.status` off the result either
/// way - the same rule `submit_job` already keeps with and without a wait.
#[test]
fn stop_job_run_without_wait_seconds_returns_the_same_job_run_shape() {
    let dir = data_dir("wait-stop-queued");
    install_job(&dir, "sleeper.yaml", SLEEPER);

    let mut server = ServerGuard::new(serve(&dir, 18237), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let submitted = client.call_tool("submit_job", json!({ "job": "sleeper" }));
    let job_run_id = serde_json::from_str::<Value>(tool_text(&submitted)).unwrap()["id"].as_i64().unwrap();

    let result = client.call_tool("stop_job_run", json!({ "job_run_id": job_run_id }));
    assert_ne!(result["isError"], json!(true), "{result}");

    let job_run: Value = serde_json::from_str(tool_text(&result)).unwrap();
    assert_eq!(job_run["id"], json!(job_run_id), "{job_run}");
    assert!(job_run.get("stop_requested").is_none(), "{job_run}");

    server.stop();
}

/// The refusal every waiting tool takes: `wait_seconds > 0` against a directory nothing is
/// serving is a tool error naming the directory, raised before any row is written - a wait
/// nothing can service must change nothing.
#[test]
fn submit_job_with_wait_seconds_on_an_unserved_directory_is_a_tool_error_naming_it() {
    let dir = data_dir("wait-submit-unserved");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("submit_job", json!({ "job": "hello", "wait_seconds": 5 }));

    assert_eq!(result["isError"], json!(true), "{result}");
    assert!(tool_text(&result).contains(&dir.to_string_lossy().to_string()), "{}", tool_text(&result));
}

/// Same refusal, reached through `get_job_run` - it never even opens `mem` before checking.
#[test]
fn get_job_run_with_wait_seconds_on_an_unserved_directory_is_a_tool_error_naming_it() {
    let dir = data_dir("wait-get-unserved");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("get_job_run", json!({ "job_run_id": 1, "wait_seconds": 5 }));

    assert_eq!(result["isError"], json!(true), "{result}");
    assert!(tool_text(&result).contains(&dir.to_string_lossy().to_string()), "{}", tool_text(&result));
}

/// Same refusal again, reached through `stop_job_run` - before the stop row is written, so
/// a run nothing will ever settle is not left carrying a stop request no server will read.
#[test]
fn stop_job_run_with_wait_seconds_on_an_unserved_directory_is_a_tool_error_naming_it() {
    let dir = data_dir("wait-stop-unserved");

    let mut client = McpClient::start(&dir);
    client.handshake();

    let result = client.call_tool("stop_job_run", json!({ "job_run_id": 1, "wait_seconds": 5 }));

    assert_eq!(result["isError"], json!(true), "{result}");
    assert!(tool_text(&result).contains(&dir.to_string_lossy().to_string()), "{}", tool_text(&result));
}
