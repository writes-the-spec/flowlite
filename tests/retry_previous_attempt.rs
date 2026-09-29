//! A retry is handed why the attempt before it failed. Driven through the built binary so
//! the log comes from a real captured stderr, through the monitor, the retry and the spawn.

use std::path::PathBuf;
use std::time::Duration;

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard};

fn data_dir() -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-retry-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

/// Fails on attempt 1 with "boom" on stderr; attempt 2 succeeds only if its log says so.
const FLAKY: &str = r#"id: flaky
name: Flaky
tasks:
  - id: work
    max_retries: 1
    retry_delay: 0
    command: 'if [ -n "$FLOWLITE_PREVIOUS_ATTEMPT_LOG" ]; then grep -q boom "$FLOWLITE_PREVIOUS_ATTEMPT_LOG"; else echo boom >&2; exit 1; fi'
"#;

#[test]
fn a_retry_reads_the_stderr_of_the_attempt_that_failed() {

    let dir = data_dir();

    install_job(&dir, "flaky.yaml", FLAKY);

    let mut server = ServerGuard::new(serve(&dir, 8141), libc::SIGTERM);

    assert!(until(Duration::from_secs(10), || is_up(&dir)), "the server never came up");

    let out = flowlite(&dir, &["job", "submit", "flaky", "--wait"]);

    server.stop();

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
