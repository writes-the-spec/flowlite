//! A run in flight when `serve` stops carries on when it starts again. Driven through the
//! built binary, since what is under test is a real SIGTERM reaching a real server, and
//! the next server finding what the first one left.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard};

fn data_dir() -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-resume-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

/// Attempt 1 says it started and then outlasts the server; any later attempt finishes at
/// once. `max_retries` is left at its default of 0, so the run only succeeds if resuming
/// does not count as a retry.
fn resumable_yaml(started: &Path) -> String {
    format!(
        "id: resumable\nname: Resumable\ntasks:\n  - id: work\n    timeout: 600\n    command: 'if [ \"$FLOWLITE_ATTEMPT\" = 1 ]; then touch {}; sleep 600; else echo resumed; fi'\n",
        started.display(),
    )
}

fn job_run_status(dir: &Path, job_run_id: &str) -> String {
    let out = flowlite(dir, &["job-run", "get", job_run_id, "--json"]);
    let job_run: Value = serde_json::from_slice(&out.stdout).unwrap_or_default();

    job_run["status"].as_str().unwrap_or_default().to_string()
}

#[test]
fn a_run_interrupted_by_a_shutdown_resumes_when_serve_starts_again() {

    let dir = data_dir();
    let started = dir.join("started");

    install_job(&dir, "resumable.yaml", &resumable_yaml(&started));

    let mut first = ServerGuard::new(serve(&dir, 8143), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the first server never came up");

    let out = flowlite(&dir, &["job", "submit", "resumable", "--json"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    let job_run: Value = serde_json::from_slice(&out.stdout).unwrap();
    let job_run_id = job_run["id"].to_string();

    assert!(until(Duration::from_secs(10), || started.exists()), "attempt 1 never started");

    first.stop();

    let mut second = ServerGuard::new(serve(&dir, 8143), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the second server never came up");

    let finished = until(Duration::from_secs(30), || job_run_status(&dir, &job_run_id) != "running");
    let status = job_run_status(&dir, &job_run_id);
    let logs = flowlite(&dir, &["job-run", "logs", &job_run_id]);

    second.stop();

    let logs = String::from_utf8_lossy(&logs.stdout);

    assert!(finished, "the run was still running after the restart");
    assert_eq!(status, "succeeded", "{logs}");
    assert!(logs.contains("serve shut down"), "{logs}");
    assert!(logs.contains("resumed"), "{logs}");
}
