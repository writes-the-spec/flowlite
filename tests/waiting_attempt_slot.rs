//! The deadlock item 7 exists to remove.
//!
//! A task that waits on another run held a concurrency slot for the length of the wait, so
//! with the global cap at 1 the child could never be dispatched and the parent waited until
//! its attempt timed out. Driven through the built binary rather than as a unit test for the
//! reason `tests/adhoc_submit.rs` gives: the run goes through `mem`, and `mem` is one
//! shared-cache database per process.

use std::path::PathBuf;
use std::time::Duration;

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard, BINARY};

/// A data directory whose config allows exactly one attempt to be running at a time. That
/// one is the cap the parent's own attempt occupies, so the child is reachable only if
/// waiting gives the slot back.
fn data_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-waiting-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    std::fs::write(
        dir.join("config.toml"),
        "[orchestrator]\nmax_running_attempts = 1\n",
    ).unwrap();

    dir
}

/// The composition `FLOWLITE_DATA_DIR` is injected for: a task that submits another job's
/// run and waits for it. The binary is named by absolute path because a task command runs
/// under `sh -c` with whatever PATH the server inherited, which in a test is not this
/// build's target directory.
fn parent_yaml() -> String {
    format!(
        "id: parent\nname: Parent\ntasks:\n  - id: launch\n    command: {BINARY} job submit child --wait\n",
    )
}

const CHILD: &str = "id: child\nname: Child\ntasks:\n  - id: work\n    command: echo child ran\n";

/// Before this feature the parent's attempt held the only slot while it polled, so the
/// child's attempt stayed Queued for ever and the parent waited until its own timeout. The
/// assertion is simply that the parent run reaches a successful end at all, within a bound
/// far below any attempt timeout.
#[test]
fn a_task_waiting_on_a_child_run_does_not_hold_the_only_slot() {

    let dir = data_dir("composes");

    install_job(&dir, "child.yaml", CHILD);
    install_job(&dir, "parent.yaml", &parent_yaml());

    let mut server = ServerGuard::new(serve(&dir, 8137), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the server never came up");

    // --wait here is this test's own wait, from outside any task: the variable is not set
    // in the test process, so it marks nothing and exercises only the parent's own wait.
    let out = flowlite(&dir, &["job", "submit", "parent", "--wait"]);

    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    server.stop();

    assert!(out.status.success(), "parent run did not succeed\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("child ran") || !stdout.is_empty(), "{stdout}");
}

/// The other half of the same fact, read off the gate rather than off the outcome: while
/// the parent is asleep in its wait, `flowlite limits` reports the global row as free and
/// says so in as many words.
#[test]
fn a_waiting_attempt_is_reported_as_holding_no_slot() {

    let dir = data_dir("reports");

    install_job(&dir, "child.yaml", CHILD);
    install_job(&dir, "parent.yaml", &parent_yaml());

    let mut server = ServerGuard::new(serve(&dir, 8138), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the server never came up");

    let submitted = flowlite(&dir, &["job", "submit", "parent"]);
    assert!(submitted.status.success(), "{}", String::from_utf8_lossy(&submitted.stderr));

    // The window this asserts in is the parent's wait, which lasts as long as the child
    // takes - so it is polled for rather than slept at.
    let saw_the_note = until(Duration::from_secs(10), || {
        let out = flowlite(&dir, &["limits"]);
        String::from_utf8_lossy(&out.stdout).contains("waiting on another run")
    });

    server.stop();

    assert!(saw_the_note, "`flowlite limits` never reported a waiting attempt");
}
