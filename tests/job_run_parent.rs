//! A run submitted by a task's command is linked to that task, and the link reads from both
//! ends. Driven through the built binary because the link travels in the environment the
//! dispatcher gives a real spawned command.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard, BINARY};

fn data_dir() -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-parent-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

/// The binary is named by absolute path because a task command runs under `sh -c` with
/// whatever PATH the server inherited, which in a test is not this build's target directory.
fn parent_yaml() -> String {
    format!(
        "id: parent\nname: Parent\ntasks:\n  - id: launch\n    timeout: 60\n    command: {BINARY} job submit child --wait\n",
    )
}

const CHILD: &str = "id: child\nname: Child\ntasks:\n  - id: work\n    command: echo child ran\n";

fn job_run_json(dir: &std::path::Path, job_run_id: i64) -> Value {
    let out = flowlite(dir, &["job-run", "get", &job_run_id.to_string(), "--json"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    serde_json::from_slice(&out.stdout).unwrap()
}

#[test]
fn a_run_submitted_by_a_task_names_its_parent_and_is_named_by_it() {

    let dir = data_dir();

    install_job(&dir, "child.yaml", CHILD);
    install_job(&dir, "parent.yaml", &parent_yaml());

    let mut server = ServerGuard::new(serve(&dir, 8139), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the server never came up");

    let out = flowlite(&dir, &["job", "submit", "parent", "--wait", "--json"]);

    server.stop();

    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    let parent_run: Value = serde_json::from_slice(&out.stdout).unwrap();
    let parent_run_id = parent_run["id"].as_i64().unwrap();

    let parent = job_run_json(&dir, parent_run_id);
    let child_ids = parent["child_job_run_ids"].as_array().unwrap();
    assert_eq!(child_ids.len(), 1, "{parent}");
    assert_eq!(parent["parent"], Value::Null, "{parent}");

    let child = job_run_json(&dir, child_ids[0].as_i64().unwrap());
    assert_eq!(child["job_id"], "child");
    assert_eq!(child["parent"]["job_run_id"], parent_run_id);
    assert_eq!(child["parent"]["task_id"], "launch");
}

const SLOW_CHILD: &str = "id: child\nname: Child\ntasks:\n  - id: work\n    command: sleep 30\n";

/// Stopping the parent stops the run its task submitted, rather than leaving it to sleep
/// out its thirty seconds on its own.
#[test]
fn stopping_a_run_stops_the_run_its_task_submitted() {

    let dir = data_dir();

    install_job(&dir, "child.yaml", SLOW_CHILD);
    install_job(&dir, "parent.yaml", &parent_yaml());

    let mut server = ServerGuard::new(serve(&dir, 8140), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the server never came up");

    let out = flowlite(&dir, &["job", "submit", "parent", "--json"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    let parent_run: Value = serde_json::from_slice(&out.stdout).unwrap();
    let parent_run_id = parent_run["id"].as_i64().unwrap();

    let mut child_run_id = None;
    let child_started = until(Duration::from_secs(10), || {
        let parent = job_run_json(&dir, parent_run_id);

        let Some(id) = parent["child_job_run_ids"][0].as_i64() else {
            return false;
        };

        child_run_id = Some(id);
        job_run_json(&dir, id)["status"] == "running"
    });
    assert!(child_started, "the child never started");

    let stopped = flowlite(&dir, &["job-run", "stop", &parent_run_id.to_string()]);
    assert!(stopped.status.success(), "{}", String::from_utf8_lossy(&stopped.stderr));

    let child_run_id = child_run_id.unwrap();
    let child_aborted = until(Duration::from_secs(10), || {
        job_run_json(&dir, child_run_id)["status"] == "aborted"
    });

    server.stop();

    assert!(child_aborted, "child ended {}", job_run_json(&dir, child_run_id)["status"]);
}
