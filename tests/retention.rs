//! Retention end to end: a served directory whose job runs more times than its
//! `keep_runs`, and the history that levels off behind it.
//!
//! Driven through the built binary rather than as a unit test because the thing under test
//! is the wiring - the `Poller` that `serve` spawns the `RetentionService` on. Every unit
//! test of that service calls `select` and `handle` itself, so all of them would still pass
//! with the service never started at all; only a real `serve` can tell those two apart. It
//! is also the one feature in this project that destroys data, which is the other reason
//! not to leave the call site uncovered.

use std::path::PathBuf;
use std::time::Duration;

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard};

/// A job that keeps only its newest two finished runs, so a handful of submits is enough
/// to cross the line - the default `keep_runs` of 100 would take a hundred.
const HELLO_KEEPING_TWO: &str = "id: hello\nname: Hello\nkeep_runs: 2\ntasks:\n  - id: say\n    \
                                 description: Says hello\n    command: echo hello\n";

fn data_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-retention-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

/// The run ids `job-run list` reports for `hello`, newest first - what a person checking
/// their history would see.
fn listed_run_ids(dir: &std::path::Path) -> Vec<i64> {
    let out = flowlite(dir, &["job-run", "list", "--job", "hello", "--json"]);

    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    let runs: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap();

    runs.iter().map(|run| run["id"].as_i64().unwrap()).collect()
}

/// Five runs of a job that keeps two: the table has to level off at two rather than grow
/// with every run, and the two left have to be the newest two - a rule that deleted from
/// the wrong end would also level off.
#[test]
fn a_served_directory_trims_a_jobs_history_to_its_keep_runs() {
    let dir = data_dir("keep-runs");
    install_job(&dir, "hello.yaml", HELLO_KEEPING_TWO);

    let mut server = ServerGuard::new(serve(&dir, 18240), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let mut submitted = Vec::new();

    for _ in 0..5 {
        let out = flowlite(&dir, &["job", "submit", "hello", "--wait", "--json"]);
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

        let run: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        submitted.push(run["id"].as_i64().unwrap());
    }

    let levelled_off = until(
        Duration::from_secs(30),
        || listed_run_ids(&dir).len() == 2,
    );

    let remaining = listed_run_ids(&dir);

    server.stop();

    assert!(levelled_off, "the history never came down to keep_runs: {remaining:?}");
    assert_eq!(remaining, vec![submitted[4], submitted[3]]);
}
