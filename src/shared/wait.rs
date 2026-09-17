//! Waiting for a run to settle, and the one precondition that makes the wait safe.
//!
//! A run is executed by the serve process rather than by whoever asked for it, so the
//! `job_run` row is the only channel between the two and polling it is the whole mechanism.
//! `--wait` and the MCP tools' `wait_seconds` are two words for this loop.

use std::path::Path;

use crate::crud::job_run::JobRun;
use crate::crud::CRUD;
use crate::serve_state::{status, ServeStatus};
use super::job_run::select_job_run;

/// Refuses a wait that nothing would ever end.
///
/// The waits below poll a row that only the serve process writes, so waiting on a data
/// directory nothing is serving blocks for ever on a row that cannot change - silently,
/// which is worse than failing. Read once, before the wait rather than on every pass: a
/// wait is allowed to span a deliberate restart of serve, which is a reasonable thing to
/// do to a server while a long run is in flight.
///
/// `Starting` counts as served. That server has the lock and will reach the row.
///
/// The refusal comes back as a typed `DataDirNotServed`, not a finished sentence - see the
/// type. Each frontend words its own remedy: the CLI's is `describe_unserved_data_dir`
/// (`src/cli/commands/job.rs`), the MCP server's is in `src/mcp/tools/result.rs`.
pub(crate) fn ensure_data_dir_is_served(data_dir: &str) -> anyhow::Result<()> {

    if matches!(status(Path::new(data_dir))?, ServeStatus::Down) {
        return Err(DataDirNotServed { data_dir: data_dir.to_string() }.into());
    }

    Ok(())
}

/// The one fact `ensure_data_dir_is_served` refuses on: nothing is serving this directory,
/// so the row a wait polls has no writer.
///
/// A typed value rather than a finished sentence, for the same reason `JobIdAlreadyInstalled`
/// is one: the remedy names what the caller would have to drop, and the callers do not agree
/// on what that is. `--wait` is a CLI flag; the MCP tools take a `wait_seconds` argument and
/// have no flags at all, so telling an agent to "drop --wait" names something it never sent.
#[derive(Debug)]
pub struct DataDirNotServed {
    pub data_dir: String,
}

impl std::fmt::Display for DataDirNotServed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Data directory {} is not being served", self.data_dir)
    }
}

impl std::error::Error for DataDirNotServed {}

/// The attempt this process is part of, if it is part of one.
///
/// `build_task_run_attempt_env` injects `FLOWLITE_TASK_RUN_ATTEMPT_ID` into every task
/// command, unconditionally and after stripping every inherited `FLOWLITE_` variable - so
/// its presence means the orchestrator spawned this process for that attempt, and a task's
/// own `env:` cannot forge it. It is inherited through `sh -c`, a wrapper script or an
/// agent shelling out, which is what lets this work without anyone parsing a command.
///
/// Absent - every wait typed at a terminal - and there is nothing to mark. Unparsable is
/// treated as absent: it is somebody's environment, not a reason to fail their wait.
fn own_task_run_attempt_id() -> Option<i64> {
    std::env::var("FLOWLITE_TASK_RUN_ATTEMPT_ID").ok()?.parse().ok()
}

/// Blocks until the run has settled, and reports it as it settled.
///
/// Signals never leave the process that publishes them, and the run is executed by the
/// serve process rather than this one, so there is nothing to subscribe to here - the
/// table is the only channel between the two, and polling it is the whole mechanism.
/// The interval is the orchestrator's own, since a run cannot settle any sooner than the
/// pass that settles it.
///
/// Inside a task, the attempt this process belongs to is marked as waiting for the length
/// of the poll, so it holds no concurrency slot while it is asleep - see
/// `src/crud/multistatements/waiting.rs`.
///
/// On cancellation: if the caller drops this future mid-wait - rmcp does exactly that when
/// an MCP client cancels - the clear never runs and the mark is left behind. That is the
/// same class as a SIGKILLed waiter: bounded by the attempt, and harmless once it settles,
/// because every tally filters on `Running` first. A `Drop` impl cannot help, since clearing
/// is async and needs the connection the loop borrows.
pub(crate) async fn wait_for_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
    poll_interval: std::time::Duration,
) -> anyhow::Result<JobRun> {

    let Some(task_run_attempt_id) = own_task_run_attempt_id() else {
        return poll_until_settled(crud, conn, job_run_id, poll_interval).await;
    };

    // Marking may decline - the attempt has settled, or never existed - and the wait is
    // still perfectly valid, so its answer only decides whether there is a mark to remove.
    let marked = crud.mark_attempt_waiting(&mut *conn, task_run_attempt_id).await?;

    let settled = poll_until_settled(crud, conn, job_run_id, poll_interval).await;

    if marked {
        // Cleared before the `?` on `settled`, so a wait that ends by raising leaves no row
        // claiming to wait for ever. A failure to clear is reported over the wait's own
        // result only when the wait itself succeeded: the run's outcome is what the caller
        // came for and outranks this bookkeeping.
        let cleared = crud.clear_attempt_waiting(&mut *conn, task_run_attempt_id).await;

        if settled.is_ok() {
            cleared?;
        }
    }

    settled
}

/// The poll itself, unchanged and unaware of any of the above.
///
/// Signals never leave the process that publishes them, and the run is executed by the
/// serve process rather than this one, so there is nothing to subscribe to here - the table
/// is the only channel between the two, and polling it is the whole mechanism. The interval
/// is the orchestrator's own, since a run cannot settle any sooner than the pass that
/// settles it.
async fn poll_until_settled(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
    poll_interval: std::time::Duration,
) -> anyhow::Result<JobRun> {

    loop {
        let job_run = select_job_run(crud, &mut *conn, job_run_id).await?;

        if job_run.status.is_finished() {
            return Ok(job_run);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::crud::job_run::{JobRunStatus, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttemptStatus};
    use crate::serve_state::ServeLock;
    use crate::test_support::TestDb;

    /// The hang this guards against: nothing is serving the directory, so the row the
    /// wait polls has no writer and the command would sit there for ever. The check itself
    /// raises the typed fact and nothing more - the remedy sentence belongs to the caller.
    #[test]
    fn waiting_on_an_unserved_data_dir_is_refused_naming_it() {

        let data_dir = std::env::temp_dir().join(format!("flowlite-unserved-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();

        let error = ensure_data_dir_is_served(&data_dir.to_string_lossy()).unwrap_err();

        assert!(error.downcast_ref::<DataDirNotServed>().is_some(), "{error:#}");
        assert!(error.to_string().contains(&data_dir.to_string_lossy().to_string()), "{error}");
    }

    /// The lock is what `serve` holds for as long as it runs, so holding it here is the
    /// directory looking served to anything that asks.
    #[test]
    fn waiting_on_a_served_data_dir_is_allowed() {

        let data_dir = std::env::temp_dir().join(format!("flowlite-served-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();

        let _lock = ServeLock::acquire(&data_dir).unwrap();

        assert!(ensure_data_dir_is_served(&data_dir.to_string_lossy()).is_ok());
    }

    #[tokio::test]
    async fn waiting_on_a_run_that_has_already_finished_returns_it_at_once() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let finished = wait_for_job_run(
            &db.crud,
            &mut conn,
            job_run.id,
            Duration::from_secs(30),
        ).await.unwrap();

        assert_eq!(finished.status, JobRunStatus::Failed);
    }

    /// The wait is a poll of the table, not a signal: the run is settled over another
    /// connection here the way the serve process settles it from another process.
    #[tokio::test]
    async fn waiting_returns_once_somebody_else_finishes_the_run() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let crud = db.crud.clone();
        let conn_pool = db.conn_pool.clone();
        let job_run_id = job_run.id;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;

            crud.update_job_runs(&*conn_pool, &UpdateJobRunsData {
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Succeeded),
                    started_at: None,
                    finished_at: None,
                },
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
            }).await.unwrap();
        });

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let finished = wait_for_job_run(
            &db.crud,
            &mut conn,
            job_run.id,
            Duration::from_millis(1),
        ).await.unwrap();

        assert_eq!(finished.status, JobRunStatus::Succeeded);
    }

    #[tokio::test]
    async fn waiting_on_a_run_that_does_not_exist_raises() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = wait_for_job_run(
            &db.crud,
            &mut conn,
            404,
            Duration::from_millis(1),
        ).await.unwrap_err().to_string();

        assert!(error.contains("404"), "{error}");
    }

    /// A wait typed at a terminal is not part of any attempt, so it must not write to the
    /// table at all - the variable's absence is the whole signal.
    #[tokio::test]
    async fn a_wait_outside_a_task_marks_nothing() {

        let _env = crate::test_support::writing_the_environment();
        unsafe { std::env::remove_var("FLOWLITE_TASK_RUN_ATTEMPT_ID") };

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let crud = db.crud.clone();
        let conn_pool = db.conn_pool.clone();
        let job_run_id = job_run.id;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            crud.update_job_runs(&*conn_pool, &UpdateJobRunsData {
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Succeeded),
                    started_at: None,
                    finished_at: None,
                },
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
            }).await.unwrap();
        });

        let mut conn = db.conn_pool.acquire().await.unwrap();
        wait_for_job_run(&db.crud, &mut conn, job_run.id, Duration::from_millis(1)).await.unwrap();

        assert_eq!(db.last_task_run_attempt(task_run.id).await.waiting_since, None);
        let _ = attempt;
    }

    /// The mark is live only for the length of the wait: set while polling, gone once the run
    /// settled and the call returned. Asserted from a second task watching the row, because
    /// by the time the wait returns the clear has already happened.
    #[tokio::test]
    async fn a_wait_inside_a_task_marks_its_own_attempt_and_clears_it() {

        let _env = crate::test_support::writing_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        unsafe { std::env::set_var("FLOWLITE_TASK_RUN_ATTEMPT_ID", attempt.id.to_string()) };

        let crud = db.crud.clone();
        let conn_pool = db.conn_pool.clone();
        let job_run_id = job_run.id;
        let task_run_id = task_run.id;
        let watcher_crud = db.crud.clone();
        let watcher_pool = db.conn_pool.clone();

        let watcher = tokio::spawn(async move {
            // Long enough for the wait to have entered its loop and marked the row.
            tokio::time::sleep(Duration::from_millis(30)).await;

            let mut conn = watcher_pool.acquire().await.unwrap();
            let attempts = watcher_crud.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: Some(task_run_id),
                    job_run_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            }).await.unwrap();

            let marked_mid_wait = attempts[0].waiting_since.is_some();

            crud.update_job_runs(&*conn_pool, &UpdateJobRunsData {
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Succeeded),
                    started_at: None,
                    finished_at: None,
                },
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
            }).await.unwrap();

            marked_mid_wait
        });

        let mut conn = db.conn_pool.acquire().await.unwrap();
        wait_for_job_run(&db.crud, &mut conn, job_run.id, Duration::from_millis(5)).await.unwrap();

        unsafe { std::env::remove_var("FLOWLITE_TASK_RUN_ATTEMPT_ID") };

        assert!(watcher.await.unwrap(), "the attempt was not marked while the wait was polling");
        assert_eq!(db.last_task_run_attempt(task_run.id).await.waiting_since, None);
    }

    /// The mark must not survive a wait that ends by raising - the run vanished, say - or a
    /// killed pipeline leaves a row claiming to be waiting for ever.
    #[tokio::test]
    async fn a_wait_that_raises_still_clears_the_mark() {

        let _env = crate::test_support::writing_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        unsafe { std::env::set_var("FLOWLITE_TASK_RUN_ATTEMPT_ID", attempt.id.to_string()) };

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = wait_for_job_run(&db.crud, &mut conn, 404, Duration::from_millis(1)).await;

        unsafe { std::env::remove_var("FLOWLITE_TASK_RUN_ATTEMPT_ID") };

        assert!(error.is_err());
        assert_eq!(db.last_task_run_attempt(task_run.id).await.waiting_since, None);
    }

    /// A variable naming an attempt that has since settled is the backgrounded-wait case. The
    /// wait itself must still work; it simply marks nothing.
    #[tokio::test]
    async fn a_wait_naming_a_settled_attempt_still_waits() {

        let _env = crate::test_support::writing_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Succeeded).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Succeeded).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Succeeded).await;

        unsafe { std::env::set_var("FLOWLITE_TASK_RUN_ATTEMPT_ID", attempt.id.to_string()) };

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let settled = wait_for_job_run(&db.crud, &mut conn, job_run.id, Duration::from_millis(1)).await.unwrap();

        unsafe { std::env::remove_var("FLOWLITE_TASK_RUN_ATTEMPT_ID") };

        assert_eq!(settled.status, JobRunStatus::Succeeded);
        assert_eq!(db.last_task_run_attempt(task_run.id).await.waiting_since, None);
    }

    /// Garbage in the variable is somebody's environment, not a reason to fail their task.
    #[tokio::test]
    async fn an_unparsable_attempt_id_is_ignored_rather_than_raised() {

        let _env = crate::test_support::writing_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;

        unsafe { std::env::set_var("FLOWLITE_TASK_RUN_ATTEMPT_ID", "not-a-number") };

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let settled = wait_for_job_run(&db.crud, &mut conn, job_run.id, Duration::from_millis(1)).await.unwrap();

        unsafe { std::env::remove_var("FLOWLITE_TASK_RUN_ATTEMPT_ID") };

        assert_eq!(settled.status, JobRunStatus::Failed);
    }
}
