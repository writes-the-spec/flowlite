//! `skip_job_run`: the pair of writes that ends a run nobody ever started - the run itself
//! and every task run it owns.
//!
//! Two services reach it, on disjoint statuses: `JobRunReleaser` for a `Scheduled` run
//! somebody stopped before its time came, and `JobRunDispatcher` for a `Queued` one. They
//! cannot race for the same row, and neither writes half of the pair itself.

use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::{JobRunStatus, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::task_run::{TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};

impl CRUD {

    /// Skips the job run and every task run it owns, none of which ever started -
    /// `started_at` is left empty on all of them, and `finished_at` set, because the run
    /// is over without anything having run.
    ///
    /// The task runs go first. A crash between the two writes then leaves the job run in
    /// the held status it was already in, so the next pass settles it again; the other
    /// order leaves a `Skipped` job run whose task runs are still `Planned`, which no
    /// service selects and nothing would ever finish.
    ///
    /// The task run update is keyed on the job run alone, deliberately, with no status
    /// filter: only a run that never started reaches this, so there is no task run status
    /// worth preserving - and one this misses is one nothing will settle.
    pub async fn skip_job_run(&self, conn: &mut SqliteConnection, job_run_id: i64) -> anyhow::Result<()> {

        let now = self.toolkit.get_current_ts();

        self.update_task_runs(
            &mut *conn,
            &UpdateTaskRunsData {
                filter: UpdateTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run_id),
                    status: None,
                },
                input: UpdateTaskRunsDataInput {
                    status: Some(TaskRunStatus::Skipped),
                    started_at: None,
                    finished_at: Some(Some(now)),
                },
            }
        ).await?;

        self.update_job_runs(
            &mut *conn,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Skipped),
                    started_at: None,
                    finished_at: Some(Some(now)),
                },
            }
        ).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::test_support::TestDb;

    #[tokio::test]
    async fn skipping_a_job_run_skips_its_task_runs_too() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Scheduled).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.skip_job_run(&mut conn, job_run.id).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Skipped);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Skipped);
    }

    /// Nothing ran, so `started_at` stays empty - but the run is over, and the retention
    /// and notification sides both read `finished_at` to know that.
    #[tokio::test]
    async fn a_skipped_job_run_is_finished_but_never_started() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Scheduled).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.skip_job_run(&mut conn, job_run.id).await.unwrap();

        let job_run = db.job_run(job_run.id).await;
        let task_run = db.task_run(task_run.id).await;

        assert!(job_run.started_at.is_none());
        assert!(job_run.finished_at.is_some());
        assert!(task_run.started_at.is_none());
        assert!(task_run.finished_at.is_some());
    }

    /// The task run update is keyed on the job run alone, with no status filter: a run
    /// this reaches never started, so there is no status of its task runs to preserve.
    #[tokio::test]
    async fn skipping_a_job_run_leaves_another_runs_task_runs_alone() {

        let db = TestDb::new().await;

        let stopped = db.insert_job_run(JobRunStatus::Scheduled).await;
        let other = db.insert_job_run(JobRunStatus::Scheduled).await;
        let other_task_run = db.insert_task_run(other.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.skip_job_run(&mut conn, stopped.id).await.unwrap();

        assert_eq!(db.job_run(other.id).await.status, JobRunStatus::Scheduled);
        assert_eq!(db.task_run(other_task_run.id).await.status, TaskRunStatus::Planned);
    }
}
