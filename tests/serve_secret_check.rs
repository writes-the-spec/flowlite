//! The behavioural properties the existence check rests on, driven through the built binary
//! the way `tests/serve_lock.rs` already does - the check itself queries `mem`, which is one
//! shared-cache database for the whole process, so it cannot be exercised as a unit test
//! without racing every other test's connection over its schema lock (see `TestDb`'s doc
//! comment in `src/test_support.rs`).
//!
//! Two properties, three tests: `serve` refuses to start when a reference is unsatisfied,
//! and the placement itself - that this cannot live inside `CRUD::init` - needs both a
//! command that walks `init` (`job list`, which must still succeed) and one that does not
//! (`job-run list`, which pins the narrower "reading a run's status needs no credentials"
//! property but, on its own, cannot tell the placement apart from the check having moved
//! into `init`).

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

mod common;
use common::ServerGuard;

const BINARY: &str = env!("CARGO_BIN_EXE_flowlite");

/// A data directory with one job whose only task names a secret nothing defines - no
/// config.toml at all, so `app_config.secrets` is empty.
fn data_dir_with_unresolvable_secret() -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-secret-check-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    std::fs::write(
        dir.join("jobs").join("nightly.yaml"),
        "id: nightly-sync\n\
         name: Nightly sync\n\
         tasks:\n\
         \x20 - id: load\n\
         \x20   description: Loads the warehouse\n\
         \x20   command: \"true\"\n\
         \x20   secret_env:\n\
         \x20     PGPASSWORD: warehouse_pw\n",
    ).unwrap();

    dir
}

/// The check itself: `serve` refuses to start rather than waiting until 03:00 to discover a
/// secret nothing defines, and names the job, the task and the secret so the fix is
/// obvious from the message alone.
#[test]
fn a_served_job_naming_an_undefined_secret_refuses_naming_job_task_and_secret() {
    let dir = data_dir_with_unresolvable_secret();

    let mut guard = ServerGuard::new(
        Command::new(BINARY)
            .args(["--data-dir", &dir.to_string_lossy(), "serve", "--port", "18210"])
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        libc::SIGTERM,
    );

    // The check runs before the bind, so a working one exits well within this - and if it
    // ever regresses into starting to listen instead, `wait_for_exit` gives up rather than
    // hanging the suite, and the guard (still holding the child) kills whatever came up.
    let status = guard.wait_for_exit(Duration::from_secs(30));

    let Some(status) = status else {
        panic!("serve did not exit on its own within 30s - the existence check appears to have regressed");
    };

    let stderr = guard.take_stderr();

    assert!(!status.success(), "serve should have refused to start:\n{stderr}");
    assert!(stderr.contains("nightly-sync"), "{stderr}");
    assert!(stderr.contains("load"), "{stderr}");
    assert!(stderr.contains("warehouse_pw"), "{stderr}");
}

/// The test that actually pins the placement: `job list` (`JobListCmd::run`)
/// calls `CRUD::init` in its own process, seeding `mem.job`/`mem.task` from the very same
/// YAML `serve` refuses above - the same `mem` the check reads. `job list` is chosen
/// precisely because it walks that `init` path, so if the existence check ever migrates
/// into `CRUD::init` itself, this is the assertion that fails.
#[test]
fn job_list_still_works_in_a_directory_serve_refuses() {
    let dir = data_dir_with_unresolvable_secret();

    let output = Command::new(BINARY)
        .args(["--data-dir", &dir.to_string_lossy(), "job", "list"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// The narrower property this one actually pins: reading a run's status never requires the
/// credentials that run used. Unlike `job_list_still_works_in_a_directory_serve_refuses`,
/// `job-run list` (`JobRunListCmd::run`) never calls `CRUD::init` at all -
/// it reads only the persisted `job_run`/`task_run` tables - so on its own this assertion
/// cannot catch a check that migrated into `CRUD::init`; that regression is what the
/// `job list` test above exists to catch.
#[test]
fn job_run_list_still_works_in_a_directory_serve_refuses() {
    let dir = data_dir_with_unresolvable_secret();

    let output = Command::new(BINARY)
        .args(["--data-dir", &dir.to_string_lossy(), "job-run", "list"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
