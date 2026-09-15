//! `delete_job_run`: the pair of writes that tombstones a run somebody removed by hand
//! before it was ever due - the run itself and every task run it owns.
//!
//! Nothing is deleted. The row stays readable, and the Scheduler's existence check is what
//! reads `Deleted` as "this occurrence is free again", so the next pass writes the
//! occurrence back under the job as it now stands. That is the whole difference from
//! `skip_job_run`, whose `Skipped` means the user cancelled the occurrence and it must
//! never come back.

use sqlx::{Connection, SqliteConnection};

use crate::crud::CRUD;
use crate::crud::job_run::{JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::task_run::{TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};

impl CRUD {

    /// Tombstones a `Submitted` job run and skips every task run it owns, and reports
    /// whether it did: a run in any other status is left exactly as it was and `false`
    /// comes back, so a caller that checked the status first still cannot act on a run
    /// that changed underneath it.
    ///
    /// That check is made *inside* the transaction, for the reason
    /// `delete_job_runs_with_children` resolves its own filter there: `JobRunReleaser` may
    /// promote a run at any moment, and a status read on the plain connection first would
    /// leave a window in which this tombstones a run at the exact instant it came due.
    ///
    /// `started_at` is left empty and `finished_at` set, on the run and on its task runs
    /// alike - nothing ever started, and the run is over.
    pub async fn delete_job_run(&self, conn: &mut SqliteConnection, job_run_id: i64) -> anyhow::Result<bool> {

        let now = self.toolkit.get_current_ts();

        let mut tx = conn.begin().await?;

        let submitted = self.select_job_run(&mut *tx, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: Some(job_run_id),
                job_id: None,
                status: Some(JobRunStatus::Submitted),
                statuses: None,
                schedule_id: None,
                scheduled_at: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        if submitted.is_none() {
            return Ok(false);
        }

        // The task runs go first, as in `skip_job_run`: the other order leaves a settled
        // job run whose task runs are still Planned, which no service selects and nothing
        // would ever finish.
        self.update_task_runs(
            &mut *tx,
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
            &mut *tx,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Deleted),
                    started_at: None,
                    finished_at: Some(Some(now)),
                },
            }
        ).await?;

        tx.commit().await?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::test_support::TestDb;

    #[tokio::test]
    async fn deleting_a_submitted_job_run_tombstones_it_and_skips_its_task_runs() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Submitted).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let deleted = db.crud.delete_job_run(&mut conn, job_run.id).await.unwrap();

        assert!(deleted);
        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Deleted);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Skipped);
    }

    /// The row is a tombstone, not a deletion: everything the run was submitted with is
    /// still there to read afterwards.
    #[tokio::test]
    async fn a_deleted_job_run_keeps_its_row_and_its_task_run_rows() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Submitted).await;
        db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.delete_job_run(&mut conn, job_run.id).await.unwrap();

        let tombstone = db.job_run(job_run.id).await;

        assert_eq!(tombstone.job_id, job_run.job_id);
        assert_eq!(tombstone.scheduled_at, job_run.scheduled_at);
        assert!(tombstone.finished_at.is_some());
        assert!(tombstone.started_at.is_none(), "nothing ever started");
    }

    /// The status guard is the whole safety of the command: a run that has been released is
    /// on its way to executing, and tombstoning it would strand whatever is already running.
    #[tokio::test]
    async fn a_job_run_that_is_no_longer_submitted_is_left_alone() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let deleted = db.crud.delete_job_run(&mut conn, job_run.id).await.unwrap();

        assert!(!deleted);
        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Running);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Running);
    }

    #[tokio::test]
    async fn deleting_a_job_run_that_is_not_there_reports_that_nothing_was_deleted() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert!(!db.crud.delete_job_run(&mut conn, 404).await.unwrap());
    }
}
