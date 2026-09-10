//! The tests that need a real second process. Everything else in flowlite is an inline
//! unit test; these live here because they run the built binary through
//! `CARGO_BIN_EXE_flowlite`, which cargo sets for integration tests only.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use flowlite::serve_state::{state_path, status, ServeStatus};

const BINARY: &str = env!("CARGO_BIN_EXE_flowlite");

/// A data directory with one job in it, so it is a real service rather than an empty
/// directory.
fn data_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-lock-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    std::fs::write(
        dir.join("jobs").join("hello.yaml"),
        "id: hello\nname: Hello\ntasks:\n  - id: say\n    \
         description: Says hello\n    command: echo hello\n",
    ).unwrap();

    dir
}

fn serve(dir: &Path, port: u16) -> Child {
    Command::new(BINARY)
        .args(["--data-dir", &dir.to_string_lossy(), "serve", "--port", &port.to_string()])
        .spawn()
        .unwrap()
}

/// Blocks until the predicate holds, so a test never sleeps a fixed guess.
fn until(timeout: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        if ready() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    false
}

fn is_up(dir: &Path) -> bool {
    matches!(status(dir), Ok(ServeStatus::Up(_)))
}

fn is_down(dir: &Path) -> bool {
    matches!(status(dir), Ok(ServeStatus::Down))
}

/// The claim the whole mechanism rests on. A pid file would still be sitting there after
/// this, naming a pid that is by now either free or somebody else's.
#[test]
fn a_sigkilled_server_reads_as_down_even_though_its_state_file_remains() {
    let dir = data_dir("sigkill");

    let mut child = serve(&dir, 18201);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    // SAFETY: kill takes two integers and touches no memory of ours.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGKILL) };
    child.wait().unwrap();

    assert!(until(Duration::from_secs(30), || is_down(&dir)));
    assert!(
        state_path(&dir).exists(),
        "the state file should still be there - the lock is what says the server is gone",
    );
}

/// The bug this closes: two servers on one directory run two schedulers over one set of
/// schedules and fire every cron twice.
#[test]
fn a_second_serve_on_one_data_dir_refuses_to_start() {
    let dir = data_dir("second");

    let mut child = serve(&dir, 18202);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let second = Command::new(BINARY)
        .args(["--data-dir", &dir.to_string_lossy(), "serve", "--port", "18203"])
        .output()
        .unwrap();

    let complaint = String::from_utf8_lossy(&second.stderr);

    assert!(!second.status.success(), "{complaint}");
    assert!(complaint.contains("already"), "{complaint}");

    // SAFETY: as above.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    child.wait().unwrap();
}

/// A directory whose server stopped tidily is as free as one that was never served, so
/// the next start needs no repair step.
#[test]
fn a_stopped_server_leaves_the_directory_startable_again() {
    let dir = data_dir("restart");

    let mut first = serve(&dir, 18204);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    // SAFETY: as above.
    unsafe { libc::kill(first.id() as libc::pid_t, libc::SIGTERM) };
    first.wait().unwrap();
    assert!(until(Duration::from_secs(30), || is_down(&dir)));

    let mut second = serve(&dir, 18204);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the restart never came up");

    // SAFETY: as above.
    unsafe { libc::kill(second.id() as libc::pid_t, libc::SIGTERM) };
    second.wait().unwrap();
}
