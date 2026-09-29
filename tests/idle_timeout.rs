//! `idle_timeout:` travels from the YAML through the run's snapshot to the monitor, and
//! ends a command that goes quiet long before its wall-clock `timeout` would.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::Value;

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard};

fn data_dir() -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-idle-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

const QUIET: &str = "id: quiet
name: Quiet
tasks:
  - id: hang
    timeout: 600
    idle_timeout: 1
    command: echo started; sleep 600
";

#[test]
fn a_task_that_goes_quiet_is_timed_out_at_its_idle_timeout() {

    let dir = data_dir();

    install_job(&dir, "quiet.yaml", QUIET);

    let mut server = ServerGuard::new(serve(&dir, 8142), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the server never came up");

    let started = Instant::now();
    let out = flowlite(&dir, &["job", "submit", "quiet", "--wait", "--json"]);
    let elapsed = started.elapsed();

    let job_run: Value = serde_json::from_slice(&out.stdout).unwrap();
    let logs = flowlite(&dir, &["job-run", "logs", &job_run["id"].to_string()]);

    server.stop();

    assert_eq!(job_run["status"], "timedout", "{job_run}");
    assert!(elapsed < Duration::from_secs(30), "took {elapsed:?}");
    assert!(String::from_utf8_lossy(&logs.stdout).contains("idle_timeout"), "{}", String::from_utf8_lossy(&logs.stdout));
}
