//! The tests that need a real second process. Everything else in flowlite is an inline
//! unit test; these live here because they run the built binary through
//! `CARGO_BIN_EXE_flowlite`, which cargo sets for integration tests only.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use flowlite::serve_state::{state_path, status, ServeStatus};

const BINARY: &str = env!("CARGO_BIN_EXE_flowlite");

/// Guard that ensures a spawned server process is killed and reaped, even if assertions fail.
/// Tolerates an already-dead process (e.g., one deliberately killed with SIGKILL).
struct ServerGuard {
    child: Child,
    signal: libc::c_int,
    /// Set once `stop` has signalled and reaped the child, so `Drop` knows the pid is no
    /// longer this child's to signal - after `wait()` returns, the OS is free to hand that
    /// pid to an unrelated process, and signalling it again would reach a stranger.
    stopped: bool,
}

impl ServerGuard {
    fn new(child: Child, signal: libc::c_int) -> Self {
        ServerGuard { child, signal, stopped: false }
    }

    /// Signals and reaps the child. Idempotent, so a test can call this itself and still
    /// let the guard's `Drop` run unconditionally without double-signalling.
    fn stop(&mut self) {
        if self.stopped {
            return;
        }

        // SAFETY: kill takes two integers and touches no memory of ours.
        unsafe { libc::kill(self.child.id() as libc::pid_t, self.signal) };

        let _ = self.child.wait();
        self.stopped = true;
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // Only reached unstopped after a test that panicked before calling `stop` - the
        // pid is still known to be this child's because nothing has reaped it yet, which
        // is exactly the guarantee `stop` itself depends on.
        self.stop();
    }
}

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

    let mut child = ServerGuard::new(serve(&dir, 18201), libc::SIGKILL);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    // Pin the other half of the feature before tearing anything down: the state file is
    // supposed to publish what actually got bound, not just whatever the test happened to
    // ask for.
    let ServeStatus::Up(state) = status(&dir).unwrap() else {
        panic!("expected the server to be up");
    };
    assert_eq!(state.pid, child.child.id());
    assert_eq!(state.port, 18201);

    child.stop();

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

    let mut child = ServerGuard::new(serve(&dir, 18202), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let second = Command::new(BINARY)
        .args(["--data-dir", &dir.to_string_lossy(), "serve", "--port", "18203"])
        .output()
        .unwrap();

    let complaint = String::from_utf8_lossy(&second.stderr);

    assert!(!second.status.success(), "{complaint}");
    assert!(complaint.contains("already"), "{complaint}");

    child.stop();
}

/// A directory whose server stopped tidily is as free as one that was never served, so
/// the next start needs no repair step.
#[test]
fn a_stopped_server_leaves_the_directory_startable_again() {
    let dir = data_dir("restart");

    let mut first = ServerGuard::new(serve(&dir, 18204), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let ServeStatus::Up(first_state) = status(&dir).unwrap() else {
        panic!("expected the first server to be up");
    };

    first.stop();
    assert!(until(Duration::from_secs(30), || is_down(&dir)));

    let mut second = ServerGuard::new(serve(&dir, 18204), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the restart never came up");

    // The case a lock alone does not save you from: without clearing the old state file
    // on acquisition, this would still read back the first server's pid for the whole
    // startup window.
    let ServeStatus::Up(second_state) = status(&dir).unwrap() else {
        panic!("expected the restarted server to be up");
    };
    assert_ne!(
        second_state.pid, first_state.pid,
        "the restart must not report the first server's pid",
    );

    second.stop();
}
