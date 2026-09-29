//! `stop_child_job_runs`: stops the runs an attempt submitted, once it has ended without
//! succeeding.

use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::{SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput, SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::TaskRunAttempt;

impl CRUD {

    /// Inserts a stop row for every unfinished run this attempt submitted that has none
    /// yet, so each is wound down exactly as a stop from any frontend would wind it down -
    /// and its own children in turn, when its attempts are settled. A child of a successful
    /// attempt is never passed here: submitting without waiting and exiting 0 hands the
    /// child off.
    pub async fn stop_child_job_runs(
        &self,
        conn: &mut SqliteConnection,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<()> {

        let children = self.select_job_runs(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: None,
                status: None,
                statuses: None,
                schedule_id: None,
                scheduled_at: None,
                parent_job_run_id: Some(task_run_attempt.job_run_id),
            },
            sort: Some(SelectJobRunsDataSort::Id),
            limit: None,
            offset: None,
        }).await?;

        // The filter matches every attempt of the run, and a retry's children are its own.
        let unfinished_children = children
            .into_iter()
            .filter(|child| child.parent_task_run_attempt_id == Some(task_run_attempt.id))
            .filter(|child| !child.status.is_finished());

        for child in unfinished_children {

            let stop = self.select_job_run_stop(&mut *conn, &SelectJobRunStopsData {
                filter: SelectJobRunStopsDataFilter {
                    id: None,
                    job_run_id: Some(child.id),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }).await?;

            if stop.is_some() {
                continue;
            }

            self.insert_job_run_stop(&mut *conn, &InsertJobRunStopData {
                input: InsertJobRunStopDataInput {
                    job_run_id: child.id,
                },
            }).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::test_support::TestDb;

    #[tokio::test]
    async fn only_the_unfinished_children_of_this_attempt_are_stopped() {

        let db = TestDb::new().await;

        let parent = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(parent.id, TaskRunStatus::Running).await;
        let failed = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Failed).await;
        let retry = db.insert_task_run_attempt(&task_run, 2, TaskRunAttemptStatus::Running).await;

        let running_child = db.insert_child_job_run(&failed, JobRunStatus::Running).await;
        let queued_child = db.insert_child_job_run(&failed, JobRunStatus::Queued).await;
        let finished_child = db.insert_child_job_run(&failed, JobRunStatus::Succeeded).await;
        let retrys_child = db.insert_child_job_run(&retry, JobRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.stop_child_job_runs(&mut conn, &failed).await.unwrap();

        assert_eq!(db.job_run_stop_count(running_child.id).await, 1);
        assert_eq!(db.job_run_stop_count(queued_child.id).await, 1);
        assert_eq!(db.job_run_stop_count(finished_child.id).await, 0);
        assert_eq!(db.job_run_stop_count(retrys_child.id).await, 0);
    }

    /// Called again for the same attempt - a settle that failed after it and is retried as
    /// invalid - it adds nothing.
    #[tokio::test]
    async fn a_child_already_stopped_is_not_stopped_again() {

        let db = TestDb::new().await;

        let parent = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(parent.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Failed).await;
        let child = db.insert_child_job_run(&attempt, JobRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.stop_child_job_runs(&mut conn, &attempt).await.unwrap();
        db.crud.stop_child_job_runs(&mut conn, &attempt).await.unwrap();

        assert_eq!(db.job_run_stop_count(child.id).await, 1);
    }
}
