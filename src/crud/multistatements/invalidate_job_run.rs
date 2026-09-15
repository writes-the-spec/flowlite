//! `invalidate_job_run`: the pair of writes that ends a run no outcome claimed - the run
//! itself and every task run it owns.
//!
//! `JobRunReleaser::set_to_invalid` is the one caller, and reaches this only as a bug:
//! a `Scheduled` run whose stopped-ness and due-ness together claimed no settle above it.

use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::{JobRunStatus, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::task_run::{TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};

impl CRUD {

    /// Invalidates the job run and every task run it owns, none of which ever started -
    /// `started_at` is left empty on all of them, and `finished_at` set, because the run
    /// is over without anything having run.
    ///
    /// The task runs go first, for the same reason `skip_job_run` orders them first: a
    /// crash between the two writes then leaves the job run in the held status it was
    /// already in, so the next pass settles it again, rather than an `Invalid` job run
    /// whose task runs are still `Planned` and nothing would ever finish.
    ///
    /// The task run update is keyed on the job run alone, with no status filter, for the
    /// same reason `skip_job_run`'s is: only a run that never started reaches this, so
    /// there is no task run status worth preserving.
    pub async fn invalidate_job_run(&self, conn: &mut SqliteConnection, job_run_id: i64) -> anyhow::Result<()> {

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
                    status: Some(TaskRunStatus::Invalid),
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
                    status: Some(JobRunStatus::Invalid),
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
    async fn invalidating_a_job_run_invalidates_its_task_runs_too() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Scheduled).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.invalidate_job_run(&mut conn, job_run.id).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Invalid);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Invalid);
    }

    /// Nothing ran, so `started_at` stays empty - but the run is over, and the retention
    /// and notification sides both read `finished_at` to know that.
    #[tokio::test]
    async fn an_invalidated_job_run_is_finished_but_never_started() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Scheduled).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.invalidate_job_run(&mut conn, job_run.id).await.unwrap();

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
    async fn invalidating_a_job_run_leaves_another_runs_task_runs_alone() {

        let db = TestDb::new().await;

        let unclaimed = db.insert_job_run(JobRunStatus::Scheduled).await;
        let other = db.insert_job_run(JobRunStatus::Scheduled).await;
        let other_task_run = db.insert_task_run(other.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.invalidate_job_run(&mut conn, unclaimed.id).await.unwrap();

        assert_eq!(db.job_run(other.id).await.status, JobRunStatus::Scheduled);
        assert_eq!(db.task_run(other_task_run.id).await.status, TaskRunStatus::Planned);
    }
}
