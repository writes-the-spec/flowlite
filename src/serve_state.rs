use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};


/// Everything a running server says about itself lives inside the data directory it is
/// serving. Nothing central: a data directory carries its own answer to "is this being
/// served", so a directory that is moved or copied stays self-describing and no record
/// anywhere else can disagree with it.
pub const STATE_DIR: &str = ".flowlite";


/// What a running `serve` says about itself. Only ever read while the lock beside it is
/// held - see `status`.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct ServeState {
    pub pid: u32,
    pub address: String,
    pub port: u16,
    pub started_at: DateTime<Utc>,
    /// Which binary is actually serving this, which an upgrade that did not restart
    /// everything otherwise leaves you guessing about.
    pub version: String,
}


/// What the lock and the state file together say about a data directory.
#[derive(Debug)]
pub enum ServeStatus {
    Down,
    /// The lock is held but no state file is readable yet: the window between a server
    /// taking the lock and its listener binding.
    Starting,
    Up(ServeState),
}


pub fn state_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(STATE_DIR)
}

pub fn lock_path(data_dir: &Path) -> PathBuf {
    state_dir(data_dir).join("serve.lock")
}

pub fn state_path(data_dir: &Path) -> PathBuf {
    state_dir(data_dir).join("serve.json")
}


/// One server's claim on one data directory, held for as long as it serves.
///
/// The lock lives in the descriptor, so dropping this releases it - which means the value
/// has to outlive the server rather than the function that took it. Bind it to a name:
/// `let _lock = ...` keeps it, `let _ = ...` drops it on the spot.
#[derive(Debug)]
pub struct ServeLock {
    /// Never read. Held so the descriptor stays open, which is what holds the lock.
    _file: File,
}


impl ServeLock {

    /// Takes the directory's lock, or reports who has it.
    pub fn acquire(data_dir: &Path) -> Result<ServeLock> {

        std::fs::create_dir_all(state_dir(data_dir)).with_context(|| format!(
            "Failed to create {} to lock the data directory",
            state_dir(data_dir).display(),
        ))?;

        let path = lock_path(data_dir);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("Failed to open the serve lock at {}", path.display()))?;

        if take_lock(&file, &path)? {
            // Winning the lock is the one moment a process can prove any existing
            // serve.json is stale - the previous holder is gone, or this acquire would
            // have failed. This is not the shutdown cleanup the design deliberately
            // skips: a crash between the lock and this line leaves the stale file for
            // the next acquirer to remove instead, and in the meantime the lock is free
            // so nothing reads the file as truth anyway. Removing it here just closes
            // the window where a new server would otherwise be reachable as `Up` while
            // still describing the old one.
            let _ = std::fs::remove_file(state_path(data_dir));
            return Ok(ServeLock { _file: file });
        }

        // The holder writes its details after taking the lock, so a server still starting
        // up has none to name yet.
        match read_state(data_dir)? {
            Some(state) => anyhow::bail!(
                "another flowlite serve is already running on {} (pid {}, http://{}:{})",
                data_dir.display(), state.pid, state.address, state.port,
            ),
            None => anyhow::bail!(
                "another flowlite serve is already starting up on {}",
                data_dir.display(),
            ),
        }
    }
}


/// Whether a server holds this data directory, asked by trying to take its lock and
/// giving it straight back.
///
/// The lock answers, never the state file: a process killed with SIGKILL leaves its state
/// file behind but cannot keep an flock, so believing the file would report a server that
/// is not there. The file is read only once the lock has proved somebody holds it.
pub fn status(data_dir: &Path) -> Result<ServeStatus> {

    let path = lock_path(data_dir);

    // Deliberately not creating it: asking whether a directory is being served must not
    // write anything into it. Read-only too: flock(LOCK_EX) succeeds on a read-only
    // descriptor, and requiring write access would fail this for a supervisor that only
    // has read access to the data directory, or a read-only mount.
    let file = match OpenOptions::new().read(true).open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ServeStatus::Down),
        Err(e) => return Err(e).with_context(|| format!(
            "Failed to open the serve lock at {}", path.display(),
        )),
    };

    // Taken means nobody was holding it. Released again by the drop at the end of this
    // function, which is why nothing else may happen in between.
    if take_lock(&file, &path)? {
        return Ok(ServeStatus::Down);
    }

    match read_state(data_dir)? {
        Some(state) => Ok(ServeStatus::Up(state)),
        None => Ok(ServeStatus::Starting),
    }
}


/// True when the lock was taken, false when somebody else holds it. Any other errno is a
/// failure to report rather than an answer to the question.
fn take_lock(file: &File, path: &Path) -> Result<bool> {

    // SAFETY: flock takes a descriptor and an int and touches nothing else. The
    // descriptor is owned by `file`, which outlives the call.
    let taken = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };

    if taken == 0 {
        return Ok(true);
    }

    let error = std::io::Error::last_os_error();

    // EWOULDBLOCK and EAGAIN are the same value on both Linux and macOS, so this one arm
    // covers "somebody has it" on either.
    match error.raw_os_error() {
        Some(libc::EWOULDBLOCK) => Ok(false),
        _ => Err(error).with_context(|| format!("Failed to lock {}", path.display())),
    }
}


pub fn write_state(data_dir: &Path, state: &ServeState) -> Result<()> {

    std::fs::create_dir_all(state_dir(data_dir))?;

    let written = serde_json::to_vec_pretty(state)
        .context("Failed to serialize the serve state")?;

    // Written beside the target and renamed over it rather than truncated in place, so a
    // concurrent `status` never catches a half-written file and reports `Starting` for a
    // server that is actually up.
    let final_path = state_path(data_dir);
    let tmp_path = final_path.with_extension("json.tmp");

    std::fs::write(&tmp_path, written).with_context(|| format!(
        "Failed to write the serve state to {}", tmp_path.display(),
    ))?;

    std::fs::rename(&tmp_path, &final_path).with_context(|| format!(
        "Failed to move the serve state into place at {}", final_path.display(),
    ))?;

    Ok(())
}


/// None for a file that is absent, or present but not yet readable as state. The second
/// is not an error: a server caught mid-write has not finished starting, which is what
/// `status` reports it as.
pub fn read_state(data_dir: &Path) -> Result<Option<ServeState>> {

    let path = state_path(data_dir);

    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!(
            "Failed to read the serve state at {}", path.display(),
        )),
    };

    Ok(serde_json::from_slice(&raw).ok())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flowlite-state-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn a_state() -> ServeState {
        ServeState {
            pid: 4242,
            address: "127.0.0.1".to_string(),
            port: 8001,
            started_at: chrono::Utc::now(),
            version: "0.1.0".to_string(),
        }
    }

    #[test]
    fn a_directory_nothing_serves_is_down() {
        assert!(matches!(status(&temp_dir()).unwrap(), ServeStatus::Down));
    }

    /// Asking a question must not answer it by writing something.
    #[test]
    fn asking_about_a_directory_nothing_serves_creates_nothing() {
        let dir = temp_dir();

        status(&dir).unwrap();

        assert!(!state_dir(&dir).exists());
    }

    #[test]
    fn a_held_lock_with_no_state_file_is_starting() {
        let dir = temp_dir();

        let _lock = ServeLock::acquire(&dir).unwrap();

        assert!(matches!(status(&dir).unwrap(), ServeStatus::Starting));
    }

    /// The restart case: a state file left behind by an earlier, now-gone process must
    /// not survive the next acquire, or `status` would report the new server as the old
    /// one for its whole startup window - a dead pid and a possibly-stale port.
    #[test]
    fn acquiring_a_free_lock_removes_a_stale_state_file_so_status_reports_starting() {
        let dir = temp_dir();

        // No lock taken here: this is what a state file looks like once its writer is
        // gone and nothing has cleaned up after it, which is the case the design leaves
        // for the next acquirer rather than for shutdown to handle.
        write_state(&dir, &a_state()).unwrap();

        let _lock = ServeLock::acquire(&dir).unwrap();

        assert!(matches!(status(&dir).unwrap(), ServeStatus::Starting));
    }

    #[test]
    fn a_held_lock_with_a_state_file_is_up_and_reports_it() {
        let dir = temp_dir();

        let _lock = ServeLock::acquire(&dir).unwrap();
        write_state(&dir, &a_state()).unwrap();

        let ServeStatus::Up(state) = status(&dir).unwrap() else {
            panic!("expected the directory to be served");
        };

        assert_eq!(state.pid, 4242);
        assert_eq!(state.port, 8001);
    }

    /// The case the whole mechanism exists for. A process that died left its state file
    /// behind but could not keep the lock, so the state file must not be believed.
    #[test]
    fn a_state_file_left_behind_by_a_dead_process_reads_as_down() {
        let dir = temp_dir();

        let lock = ServeLock::acquire(&dir).unwrap();
        write_state(&dir, &a_state()).unwrap();
        drop(lock);

        assert!(state_path(&dir).exists());
        assert!(matches!(status(&dir).unwrap(), ServeStatus::Down));
    }

    #[test]
    fn a_second_acquire_is_refused_naming_the_holder() {
        let dir = temp_dir();

        let _lock = ServeLock::acquire(&dir).unwrap();
        write_state(&dir, &a_state()).unwrap();

        let error = ServeLock::acquire(&dir).unwrap_err().to_string();

        assert!(error.contains("4242"), "{error}");
        assert!(error.contains("8001"), "{error}");
        assert!(error.contains(&dir.to_string_lossy().to_string()), "{error}");
    }

    /// Refused even before the holder has written its details, since the lock alone is
    /// what says somebody is there.
    #[test]
    fn a_second_acquire_is_refused_before_the_holder_has_written_its_state() {
        let dir = temp_dir();

        let _lock = ServeLock::acquire(&dir).unwrap();

        assert!(ServeLock::acquire(&dir).is_err());
    }

    #[test]
    fn a_released_lock_can_be_taken_again() {
        let dir = temp_dir();

        let lock = ServeLock::acquire(&dir).unwrap();
        drop(lock);

        assert!(ServeLock::acquire(&dir).is_ok());
    }

    #[test]
    fn state_round_trips_through_the_file() {
        let dir = temp_dir();
        let state = a_state();

        write_state(&dir, &state).unwrap();

        assert_eq!(read_state(&dir).unwrap().unwrap(), state);
    }

    #[test]
    fn no_state_file_reads_as_no_state() {
        assert!(read_state(&temp_dir()).unwrap().is_none());
    }

    /// A half-written file is a server that has not finished starting, not an error to
    /// report - which is why `status` can answer Starting rather than failing.
    #[test]
    fn an_unreadable_state_file_reads_as_no_state() {
        let dir = temp_dir();
        std::fs::create_dir_all(state_dir(&dir)).unwrap();
        std::fs::write(state_path(&dir), b"{\"pid\": ").unwrap();

        assert!(read_state(&dir).unwrap().is_none());
    }
}
