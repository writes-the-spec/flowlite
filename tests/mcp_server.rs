//! `flowlite mcp` — the handshake, over the same pipes an MCP client would use.
//!
//! Driven through the built binary rather than as unit tests because the thing under test
//! is the process: that the server reaches stdin and stdout at all, that *nothing else in
//! the binary writes a byte to stdout* and corrupts the framing, and that closing stdin
//! ends it. None of the three is observable from inside the library.
//!
//! MCP over stdio is newline-delimited JSON-RPC 2.0: one JSON object per line in, one per
//! line out. Later cuts add the tools to this file, so the helpers here take a method and
//! params rather than knowing any particular call.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use serde_json::{json, Value};

mod common;
use common::ServerGuard;

const BINARY: &str = env!("CARGO_BIN_EXE_flowlite");

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

    /// Closes stdin, which is how an MCP client stops a server it spawned, and reports how
    /// the process ended.
    fn close_stdin(&mut self) {
        self.stdin.take();
    }
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

/// The capability is what makes a client ask for tools at all, so an empty list has to be
/// reached by declaring tools and having none - not by declining to have the capability.
#[test]
fn the_handshake_declares_tools_and_lists_none_yet() {
    let dir = data_dir("tools");
    let mut client = McpClient::start(&dir);

    let initialized = client.initialize();
    assert!(
        initialized["capabilities"]["tools"].is_object(),
        "the tools capability was not declared: {initialized}",
    );

    client.notify("notifications/initialized");

    let listed = client.request("tools/list", json!({}));

    assert_eq!(listed["tools"], json!([]), "a tool is registered in this cut");
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
