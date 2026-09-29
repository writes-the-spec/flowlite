use std::path::{Path, PathBuf};
use anyhow::Context;

use crate::serve_state::STATE_DIR;


/// Where every run's directory lives, under the data directory's own state directory
/// rather than at its top level: the data directory is walked for YAML, so a name
/// reserved there would be a name a user cannot give a file.
const RUNS_DIR: &str = "runs";

/// The one reserved name inside a run's directory - which is also the working directory of
/// every task that declares none, so what a task can see there is its own business. Hidden
/// so a task listing its directory sees only its own files, and so `rm -rf *` in it cannot
/// take the run's results with it.
const OUTPUT_DIR: &str = ".output";


/// The directory one job run works in. Created when the run starts, used as the working
/// directory of every task that declares none, deleted with the run by retention.
///
/// Built from the **canonicalised** data directory. `data_dir` defaults to `.` and may be
/// relative, and a relative working directory would be resolved against the server's own
/// cwd - the one thing this directory exists to stop mattering. Canonicalising needs the
/// data directory to exist, which it does wherever this is called: nothing reaches a job
/// run without one.
pub fn job_run_dir(data_dir: &str, job_run_id: i64) -> anyhow::Result<PathBuf> {

    let canonical_data_dir = std::fs::canonicalize(data_dir)
        .with_context(|| format!("Failed to resolve the data directory '{}'", data_dir))?;

    Ok(canonical_data_dir
        .join(STATE_DIR)
        .join(RUNS_DIR)
        .join(job_run_id.to_string()))
}

/// The file one attempt writes its result to, named for the attempt rather than the task.
///
/// A stable per-task name would let a retry inherit the last attempt's bytes - attempt 1
/// writes a result and fails, attempt 2 writes nothing and succeeds, and the task's result
/// is attempt 1's. This makes that impossible rather than leaving it to an unlink somebody
/// has to remember. It also leaves a failed attempt's result on disk, which is how a
/// retry is handed it as `FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT`.
pub fn task_output_path(job_run_dir: &Path, task_id: &str, attempt: u32) -> PathBuf {
    job_run_dir.join(OUTPUT_DIR).join(format!("{}.{}", task_id, attempt))
}

/// The log a retry is handed of the attempt before it - that attempt's stdout and stderr as
/// they were captured - kept beside its result and named for it the same way.
pub fn task_log_path(job_run_dir: &Path, task_id: &str, attempt: u32) -> PathBuf {
    job_run_dir.join(OUTPUT_DIR).join(format!("{}.{}.log", task_id, attempt))
}

/// Creates a run's directory and the output directory inside it, so a task can write to
/// `$FLOWLITE_TASK_OUTPUT` without creating anything first.
///
/// Both at once, and `create_dir_all` rather than `create_dir`, so a restart that finds the
/// directory already there is not an error: the dispatcher may have created it and crashed
/// before writing the status it was about to write.
pub fn create_job_run_dir(data_dir: &str, job_run_id: i64) -> anyhow::Result<PathBuf> {

    let dir = job_run_dir(data_dir, job_run_id)?;

    std::fs::create_dir_all(dir.join(OUTPUT_DIR))
        .with_context(|| format!("Failed to create the run directory {}", dir.display()))?;

    Ok(dir)
}

/// Removes a run's directory, and says whether there was one.
///
/// Called after the rows are gone, so it can only ever be told about a failure - see
/// `RetentionService::handle`.
pub fn remove_job_run_dir(data_dir: &str, job_run_id: i64) -> anyhow::Result<bool> {

    let dir = job_run_dir(data_dir, job_run_id)?;

    if !dir.exists() {
        return Ok(false);
    }

    std::fs::remove_dir_all(&dir)
        .with_context(|| format!("Failed to remove the run directory {}", dir.display()))?;

    Ok(true)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn temp_data_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flowlite-run-dir-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::canonicalize(&dir).unwrap()
    }

    #[test]
    fn a_runs_directory_sits_under_the_data_directorys_state_directory() {
        let data_dir = temp_data_dir();

        let dir = job_run_dir(&data_dir.to_string_lossy(), 42).unwrap();

        assert_eq!(dir, data_dir.join(".flowlite").join("runs").join("42"));
    }

    /// `data_dir` may be relative - `.` is its default - and a relative working directory
    /// would be resolved against the server's own cwd rather than against the data
    /// directory. The relative path here is made against the cwd rather than by changing
    /// it: the cwd is process-wide, and every other test in this run shares it.
    #[test]
    fn the_path_is_absolute_even_when_the_data_directory_is_relative() {
        let name = format!("flowlite-run-dir-relative-{}", uuid::Uuid::new_v4());
        std::fs::create_dir_all(&name).unwrap();

        let dir = job_run_dir(&name, 42);

        std::fs::remove_dir_all(&name).unwrap();

        let dir = dir.unwrap();

        assert!(dir.is_absolute(), "{}", dir.display());
        assert!(dir.ends_with("42"), "{}", dir.display());
    }

    #[test]
    fn creating_a_run_directory_creates_the_output_directory_inside_it() {
        let data_dir = temp_data_dir();

        let dir = create_job_run_dir(&data_dir.to_string_lossy(), 7).unwrap();

        assert!(dir.is_dir());
        assert!(dir.join(".output").is_dir());
    }

    /// The dispatcher creates the directory before it writes the status that says the run
    /// started, so a crash between the two brings it back to a directory that is already
    /// there.
    #[test]
    fn creating_a_run_directory_twice_is_not_an_error() {
        let data_dir = temp_data_dir();

        create_job_run_dir(&data_dir.to_string_lossy(), 7).unwrap();
        create_job_run_dir(&data_dir.to_string_lossy(), 7).unwrap();
    }

    #[test]
    fn an_output_path_names_the_task_and_the_attempt() {
        let dir = PathBuf::from("/srv/flowlite/.flowlite/runs/7");

        assert_eq!(
            task_output_path(&dir, "extract", 2),
            PathBuf::from("/srv/flowlite/.flowlite/runs/7/.output/extract.2"),
        );
    }

    #[test]
    fn removing_a_run_directory_removes_what_is_in_it_and_leaves_its_neighbour() {
        let data_dir = temp_data_dir();

        let dir = create_job_run_dir(&data_dir.to_string_lossy(), 7).unwrap();
        let neighbour = create_job_run_dir(&data_dir.to_string_lossy(), 8).unwrap();
        std::fs::write(dir.join("work.txt"), "something").unwrap();

        assert!(remove_job_run_dir(&data_dir.to_string_lossy(), 7).unwrap());

        assert!(!dir.exists());
        assert!(neighbour.is_dir());
    }

    #[test]
    fn removing_a_run_directory_that_was_never_created_says_so_rather_than_failing() {
        let data_dir = temp_data_dir();

        assert!(!remove_job_run_dir(&data_dir.to_string_lossy(), 7).unwrap());
    }
}
