//! Shared across the integration tests in this directory - each file under `tests/` is
//! its own binary, so anything more than one of them needs lives here instead of being
//! copy-pasted per file. `tests/common/mod.rs` (rather than `tests/common.rs`) is the name
//! cargo will not itself pick up as a third test binary.
//!
//! Each test binary that includes this module only calls the methods it needs, so the ones
//! the *other* binary happens to need would otherwise warn as dead code here - hence the
//! blanket allow, rather than one per binary's unused subset.
#![allow(dead_code)]

use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output};
use std::time::{Duration, Instant};

use flowlite::serve_state::{status, ServeStatus};

/// The binary every integration test drives - `CARGO_BIN_EXE_flowlite`, which cargo sets
/// for integration tests only, hence the `const` rather than reading it from `PATH`.
pub const BINARY: &str = env!("CARGO_BIN_EXE_flowlite");

/// Spawns `flowlite serve` against `dir` on `port`, left running - the caller wraps the
/// result in a `ServerGuard` so it is signalled and reaped even if the test panics.
pub fn serve(dir: &Path, port: u16) -> Child {
    Command::new(BINARY)
        .args(["--data-dir", &dir.to_string_lossy(), "serve", "--port", &port.to_string()])
        .spawn()
        .unwrap()
}

/// Runs `flowlite <args>` against `dir` to completion, the same way a person at a terminal
/// would, and returns what it printed and how it exited.
pub fn flowlite(dir: &Path, args: &[&str]) -> Output {
    Command::new(BINARY)
        .args(["--data-dir", &dir.to_string_lossy()])
        .args(args)
        .output()
        .unwrap()
}

/// Writes a job file into `dir/jobs/`, installing it the way a person editing the data
/// directory by hand would - as opposed to a file written elsewhere and submitted with
/// `-f`, which is never installed.
pub fn install_job(dir: &Path, name: &str, yaml: &str) {
    std::fs::write(dir.join("jobs").join(name), yaml).unwrap();
}

/// Blocks until the predicate holds, so a test never sleeps a fixed guess at how long a
/// spawned process takes to become ready.
pub fn until(timeout: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        if ready() {
            return true;
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    false
}

pub fn is_up(dir: &Path) -> bool {
    matches!(status(dir), Ok(ServeStatus::Up(_)))
}

/// Guard that ensures a spawned server process is killed and reaped, even if assertions fail.
/// Tolerates an already-dead process (e.g., one deliberately killed with SIGKILL).
pub struct ServerGuard {
    child: Child,
    signal: libc::c_int,
    /// Set once the child is known reaped - either `stop` signalled and reaped it, or
    /// `wait_for_exit` observed it exit on its own - so `Drop` knows the pid is no longer
    /// this child's to signal. Once a wait call returns, the OS is free to hand that pid to
    /// an unrelated process, and signalling it again would reach a stranger.
    stopped: bool,
}

impl ServerGuard {
    pub fn new(child: Child, signal: libc::c_int) -> Self {
        ServerGuard { child, signal, stopped: false }
    }

    /// The pid of the spawned process, for a test asserting a state file names it.
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// Signals and reaps the child. Idempotent, so a test can call this itself and still
    /// let the guard's `Drop` run unconditionally without double-signalling.
    pub fn stop(&mut self) {
        if self.stopped {
            return;
        }

        // SAFETY: kill takes two integers and touches no memory of ours.
        unsafe { libc::kill(self.child.id() as libc::pid_t, self.signal) };

        let _ = self.child.wait();
        self.stopped = true;
    }

    /// Polls for the child to exit on its own, without ever blocking past `timeout` -
    /// unlike `Child::wait`/`Output`, which would hang the whole suite if the exit this
    /// test expects ever regressed into the process actually binding and serving forever.
    /// Returns `None` if the deadline passed first; the caller is still responsible for
    /// calling (or dropping into) `stop` to clean up whatever came up instead.
    pub fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                // try_wait reaps the child exactly like wait() does once it reports an
                // exit, so the pid is free the instant this returns - stop()/Drop must not
                // signal it again.
                self.stopped = true;
                return Some(status);
            }

            std::thread::sleep(Duration::from_millis(50));
        }

        None
    }

    /// Reads whatever the child wrote to stderr to EOF. Only sound to call once the child
    /// has exited (see `wait_for_exit`) and only if it was spawned with
    /// `.stderr(Stdio::piped())` - otherwise there is no pipe to take.
    pub fn take_stderr(&mut self) -> String {
        let mut buf = String::new();

        self.child.stderr.take()
            .expect("stderr was not piped for this child")
            .read_to_string(&mut buf)
            .unwrap();

        buf
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
