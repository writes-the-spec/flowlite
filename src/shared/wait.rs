//! Waiting for a run to settle, and the one precondition that makes the wait safe.
//!
//! A run is executed by the serve process rather than by whoever asked for it, so the
//! `job_run` row is the only channel between the two and polling it is the whole mechanism.
//! `--wait` and the MCP tools' `wait_seconds` are two words for this loop.

use std::path::Path;

use chrono::{DateTime, Utc};

use crate::crud::job_run::JobRun;
use crate::crud::task_run_attempt::{
    TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter,
    UpdateTaskRunAttemptsDataInput,
};
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

/// The attempt this process runs inside, if any. The dispatcher sets this variable on every
/// task command after stripping every inherited `FLOWLITE_` variable, so a task's own `env:`
/// cannot forge it.
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
/// With a `bound`, the run is returned unfinished once it elapses - never as an error, so
/// the caller keeps the id to ask again.
///
/// Inside a task, the task's own attempt is marked as waiting on every pass of the poll,
/// so it holds no concurrency slot while it sleeps. The bound is applied inside the poll
/// rather than around this call, so that elapsing still clears the mark. A cancelled wait
/// does leave it behind, which stops mattering once the attempt settles.
pub(crate) async fn wait_for_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
    poll_interval: std::time::Duration,
    bound: Option<std::time::Duration>,
) -> anyhow::Result<JobRun> {

    wait_as_attempt(crud, conn, job_run_id, poll_interval, bound, own_task_run_attempt_id()).await
}

async fn wait_as_attempt(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
    poll_interval: std::time::Duration,
    bound: Option<std::time::Duration>,
    task_run_attempt_id: Option<i64>,
) -> anyhow::Result<JobRun> {

    let polled = match bound {

        None => poll_until_settled(crud, conn, job_run_id, poll_interval, task_run_attempt_id).await,

        Some(bound) => {
            let bounded = tokio::time::timeout(
                bound,
                poll_until_settled(crud, conn, job_run_id, poll_interval, task_run_attempt_id),
            ).await;

            match bounded {
                Ok(settled) => settled,
                Err(_elapsed) => select_job_run(crud, conn, job_run_id).await,
            }
        }
    };

    if let Some(task_run_attempt_id) = task_run_attempt_id {
        set_waiting_since(crud, conn, task_run_attempt_id, None).await;
    }

    polled
}

/// Only a Running attempt is written: the id may come from a backgrounded wait that
/// outlived its task, and every tally ignores a settled row anyway. A failure is reported
/// rather than raised, since this is bookkeeping and must not fail the wait itself.
async fn set_waiting_since(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    task_run_attempt_id: i64,
    waiting_since: Option<DateTime<Utc>>,
) {

    let updated = crud.update_task_run_attempts(&mut *conn, &UpdateTaskRunAttemptsData {
        input: UpdateTaskRunAttemptsDataInput {
            status: None,
            started_at: None,
            finished_at: None,
            process_group_id: None,
            output: None,
            waiting_since: Some(waiting_since),
        },
        filter: UpdateTaskRunAttemptsDataFilter {
            id: Some(task_run_attempt_id),
            task_run_id: None,
            status: Some(TaskRunAttemptStatus::Running),
        },
    }).await;

    if let Err(err) = updated {
        eprintln!(
            "Task run attempt {task_run_attempt_id} could not record whether it is waiting on \
             another run, so the concurrency limits may count it wrongly: {err:?}",
        );
    }
}

async fn poll_until_settled(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
    poll_interval: std::time::Duration,
    task_run_attempt_id: Option<i64>,
) -> anyhow::Result<JobRun> {

    let waiting_since = Utc::now();

    loop {
        let job_run = select_job_run(crud, &mut *conn, job_run_id).await?;

        if job_run.status.is_finished() {
            return Ok(job_run);
        }

        // Stamped on every pass rather than once: two waits in one task share the mark,
        // and the first to return clears it while the other is still asleep.
        if let Some(task_run_attempt_id) = task_run_attempt_id {
            set_waiting_since(crud, conn, task_run_attempt_id, Some(waiting_since)).await;
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
            None,
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
            None,
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
            None,
        ).await.unwrap_err().to_string();

        assert!(error.contains("404"), "{error}");
    }

    /// An unbounded wait on `job_run_id` from inside `task_run_attempt_id`, polling every 5ms.
    fn spawn_wait(
        db: &TestDb,
        job_run_id: i64,
        task_run_attempt_id: i64,
    ) -> tokio::task::JoinHandle<anyhow::Result<JobRun>> {

        let crud = db.crud.clone();
        let conn_pool = db.conn_pool.clone();

        tokio::spawn(async move {
            let mut conn = conn_pool.acquire().await.unwrap();
            let poll_interval = Duration::from_millis(5);

            wait_as_attempt(&crud, &mut conn, job_run_id, poll_interval, None, Some(task_run_attempt_id)).await
        })
    }

    async fn succeed_job_run(db: &TestDb, job_run_id: i64) {
        db.crud.update_job_runs(&*db.conn_pool, &UpdateJobRunsData {
            input: UpdateJobRunsDataInput {
                status: Some(JobRunStatus::Succeeded),
                started_at: None,
                finished_at: None,
            },
            filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
        }).await.unwrap();
    }

    /// The mark is live only for the length of the wait: set while polling, gone once the
    /// run settled and the call returned.
    #[tokio::test]
    async fn a_wait_inside_a_task_marks_its_own_attempt_and_clears_it() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let wait = spawn_wait(&db, job_run.id, attempt.id);

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(db.task_run_attempt(attempt.id).await.waiting_since.is_some());

        succeed_job_run(&db, job_run.id).await;

        wait.await.unwrap().unwrap();

        assert_eq!(db.task_run_attempt(attempt.id).await.waiting_since, None);
    }

    /// The first of two waits in one task to return clears the shared mark; the other puts
    /// it back on its next pass rather than leaving the attempt counted while it sleeps.
    #[tokio::test]
    async fn a_second_wait_in_the_same_task_restores_the_mark_the_first_cleared() {

        let db = TestDb::new().await;

        let first_run = db.insert_job_run(JobRunStatus::Running).await;
        let second_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(first_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let first_wait = spawn_wait(&db, first_run.id, attempt.id);
        let second_wait = spawn_wait(&db, second_run.id, attempt.id);

        tokio::time::sleep(Duration::from_millis(30)).await;
        succeed_job_run(&db, first_run.id).await;
        first_wait.await.unwrap().unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(db.task_run_attempt(attempt.id).await.waiting_since.is_some());

        succeed_job_run(&db, second_run.id).await;
        second_wait.await.unwrap().unwrap();

        assert_eq!(db.task_run_attempt(attempt.id).await.waiting_since, None);
    }

    #[tokio::test]
    async fn a_wait_that_raises_still_clears_the_mark() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let waited = wait_as_attempt(
            &db.crud,
            &mut conn,
            404,
            Duration::from_millis(1),
            None,
            Some(attempt.id),
        ).await;

        assert!(waited.is_err());
        assert_eq!(db.task_run_attempt(attempt.id).await.waiting_since, None);
    }

    /// An elapsed bound is the MCP tools' ordinary outcome, so it must clear the mark
    /// exactly as a settled run does.
    #[tokio::test]
    async fn a_bound_that_elapses_clears_the_mark_and_returns_the_run_unfinished() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let unfinished = wait_as_attempt(
            &db.crud,
            &mut conn,
            job_run.id,
            Duration::from_millis(5),
            Some(Duration::from_millis(50)),
            Some(attempt.id),
        ).await.unwrap();

        assert_eq!(unfinished.status, JobRunStatus::Running);
        assert_eq!(db.task_run_attempt(attempt.id).await.waiting_since, None);
    }

    /// A backgrounded wait can outlive the attempt whose id it inherited. It still waits,
    /// but must not stamp the settled row.
    #[tokio::test]
    async fn a_wait_naming_a_settled_attempt_waits_without_marking_it() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let unfinished = wait_as_attempt(
            &db.crud,
            &mut conn,
            job_run.id,
            Duration::from_millis(5),
            Some(Duration::from_millis(30)),
            Some(attempt.id),
        ).await.unwrap();

        assert_eq!(unfinished.status, JobRunStatus::Running);
        assert_eq!(db.task_run_attempt(attempt.id).await.waiting_since, None);
    }
}
