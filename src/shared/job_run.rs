use serde::Serialize;

use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput};
use crate::crud::task_run::TaskRun;
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::crud::CRUD;
use super::format;

/// A run and the task runs under it, flattened so that `.status` reads off the run itself
/// - the same field name `job submit --json` prints, rather than one nested a level down.
#[derive(Serialize)]
pub(crate) struct JobRunDetail {
    #[serde(flatten)]
    pub(crate) job_run: JobRun,
    pub(crate) task_runs: Vec<TaskRun>,
}

/// One attempt with what it wrote. The MCP `get_job_run_logs` tool fills the same shape
/// with `stdout`/`stderr` truncated rather than copied whole.
#[derive(Serialize)]
pub(crate) struct TaskRunAttemptLog {
    #[serde(flatten)]
    pub(crate) task_run_attempt: TaskRunAttempt,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

/// The run, or an error naming the id nothing matched.
pub(crate) async fn select_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
) -> anyhow::Result<JobRun> {

    let job_run = crud.select_job_run(&mut *conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: Some(job_run_id),
            job_id: None,
            status: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await?;

    match job_run {
        Some(job_run) => Ok(job_run),
        None => anyhow::bail!("Job run {} not found", job_run_id),
    }
}

/// Asks for a run to be stopped by writing the row the orchestrator reads. Nothing is
/// killed here - this process owns no child, and the serve process acts on the row on a
/// later pass of its own.
///
/// A settled run is refused rather than accepted: its row would never be read, and a
/// caller told the stop succeeded would have been told something untrue.
pub(crate) async fn stop_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
) -> anyhow::Result<()> {

    let job_run = crud.select_job_run(&mut *conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: Some(job_run_id),
            job_id: None,
            status: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await?;

    let Some(job_run) = job_run else {
        anyhow::bail!("Job run {} not found", job_run_id);
    };

    if job_run.status.is_finished() {
        anyhow::bail!(
            "Job run {} has already {}",
            job_run_id,
            format::job_run_word(job_run.status),
        );
    }

    crud.insert_job_run_stop(&mut *conn, &InsertJobRunStopData {
        input: InsertJobRunStopDataInput { job_run_id },
    }).await?;

    Ok(())
}

/// The words a run's status is stored and filtered by, so `job-run list --status` and the
/// MCP `list_job_runs` tool accept exactly what the UI's own filter does.
///
/// Derived from `JobRunStatus::ALL` rather than matched by hand: a status added to the
/// enum reaches this parser and its error message without anyone remembering to come here.
pub(crate) fn parse_job_run_status(raw: &str) -> Result<JobRunStatus, String> {

    let status = JobRunStatus::ALL
        .into_iter()
        .find(|status| status.to_string() == raw);

    match status {
        Some(status) => Ok(status),
        None => Err(format!("unknown status '{}', expected one of {}", raw, accepted_statuses())),
    }
}

fn accepted_statuses() -> String {
    JobRunStatus::ALL
        .iter()
        .map(|status| status.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::{UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
    use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
    use crate::shared::wait::wait_for_job_run;
    use crate::test_support::TestDb;

    #[test]
    fn a_status_word_parses_to_its_status() {
        assert_eq!(parse_job_run_status("running").unwrap(), JobRunStatus::Running);
        assert_eq!(parse_job_run_status("succeeded").unwrap(), JobRunStatus::Succeeded);
    }

    /// The same spelling the UI filters by and the same one serde writes, so the two
    /// surfaces name a status alike.
    #[test]
    fn a_timed_out_run_is_spelled_the_way_it_is_stored() {
        assert_eq!(parse_job_run_status("timedout").unwrap(), JobRunStatus::TimedOut);
    }

    /// The parser reads the same list the UI's filter chips are built from, so a status
    /// added to the enum cannot reach one surface and not the other - which it did when
    /// `Invalid` was added and this parser still refused the word.
    #[test]
    fn every_status_the_ui_can_filter_by_parses() {
        for status in JobRunStatus::ALL {
            assert_eq!(parse_job_run_status(&status.to_string()).unwrap(), status);
        }
    }

    #[test]
    fn an_invalid_status_parses() {
        assert_eq!(parse_job_run_status("invalid").unwrap(), JobRunStatus::Invalid);
    }

    #[test]
    fn an_unknown_status_is_refused_naming_what_is_accepted() {
        let error = parse_job_run_status("exploded").unwrap_err();

        assert!(error.contains("exploded"), "{error}");
        assert!(error.contains("succeeded"), "{error}");
        assert!(error.contains("timedout"), "{error}");
    }

    /// The stop is a row, not a signal: this command writes it from one process and the
    /// orchestrator reads it from another, so what this asserts is the row being there.
    #[tokio::test]
    async fn a_running_job_run_is_stopped() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        stop_job_run(&db.crud, &mut conn, job_run.id).await.unwrap();

        assert!(job_run_stop(&db, job_run.id).await.is_some());
    }

    /// A settled run has nothing left to stop, and the row would sit in the table for ever
    /// unread. Saying so beats reporting a success that did nothing.
    #[tokio::test]
    async fn a_finished_job_run_cannot_be_stopped() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = stop_job_run(&db.crud, &mut conn, job_run.id).await.unwrap_err().to_string();

        assert!(error.contains(&job_run.id.to_string()), "{error}");
        assert!(error.contains("succeeded"), "{error}");

        assert!(job_run_stop(&db, job_run.id).await.is_none(), "a refused stop still wrote a row");
    }

    /// A run id nothing matches is the caller's mistake, not a silent no-op.
    #[tokio::test]
    async fn stopping_a_job_run_that_does_not_exist_raises() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = stop_job_run(&db.crud, &mut conn, 404).await.unwrap_err().to_string();

        assert!(error.contains("404"), "{error}");
    }

    /// What `--wait` runs end to end: the row goes in, and the wait returns only once
    /// somebody else settles the run - the serve process, here another connection. The
    /// stop is still on the table while the wait is in progress, which is what lets the
    /// serve process see it at all.
    #[tokio::test]
    async fn stopping_with_wait_returns_once_the_run_settles() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        stop_job_run(&db.crud, &mut conn, job_run.id).await.unwrap();

        let crud = db.crud.clone();
        let conn_pool = db.conn_pool.clone();
        let job_run_id = job_run.id;

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;

            crud.update_job_runs(&*conn_pool, &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Aborted),
                    started_at: None,
                    finished_at: None,
                },
            }).await.unwrap();
        });

        let settled = wait_for_job_run(
            &db.crud,
            &mut conn,
            job_run.id,
            std::time::Duration::from_millis(1),
        ).await.unwrap();

        assert_eq!(settled.status, JobRunStatus::Aborted);
        assert!(job_run_stop(&db, job_run.id).await.is_some());
    }

    async fn job_run_stop(db: &TestDb, job_run_id: i64) -> Option<crate::crud::job_run_stop::JobRunStop> {
        db.crud.select_job_run_stop(&*db.conn_pool, &SelectJobRunStopsData {
            filter: SelectJobRunStopsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap()
    }
}
