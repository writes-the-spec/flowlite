//! `select_job_run_descendants`: every run a run's tasks submitted, and every run theirs did.

use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};

impl CRUD {

    /// Each run comes after the run that submitted it, so the reverse is an order that
    /// deletes a child before the attempt it references. The run itself is not included.
    pub async fn select_job_run_descendants(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
    ) -> anyhow::Result<Vec<JobRun>> {

        let mut descendants: Vec<JobRun> = Vec::new();
        let mut parent_ids = vec![job_run_id];

        while let Some(parent_id) = parent_ids.pop() {

            let children = self.select_job_runs(&mut *conn, &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: None,
                    statuses: None,
                    schedule_id: None,
                    scheduled_at: None,
                    parent_job_run_id: Some(parent_id),
                },
                sort: Some(SelectJobRunsDataSort::Id),
                limit: None,
                offset: None,
            }).await?;

            parent_ids.extend(children.iter().map(|child| child.id));
            descendants.extend(children);
        }

        Ok(descendants)
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::test_support::TestDb;

    #[tokio::test]
    async fn descendants_include_grandchildren_and_nothing_unrelated() {

        let db = TestDb::new().await;

        let root = db.insert_job_run(JobRunStatus::Running).await;
        let root_task_run = db.insert_task_run(root.id, TaskRunStatus::Running).await;
        let root_attempt = db.insert_task_run_attempt(&root_task_run, 1, TaskRunAttemptStatus::Running).await;

        let child = db.insert_child_job_run(&root_attempt, JobRunStatus::Running).await;
        let child_task_run = db.insert_task_run(child.id, TaskRunStatus::Running).await;
        let child_attempt = db.insert_task_run_attempt(&child_task_run, 1, TaskRunAttemptStatus::Running).await;

        let grandchild = db.insert_child_job_run(&child_attempt, JobRunStatus::Running).await;
        db.insert_job_run(JobRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let descendants = db.crud.select_job_run_descendants(&mut conn, root.id).await.unwrap();

        let ids: Vec<i64> = descendants.iter().map(|job_run| job_run.id).collect();
        assert_eq!(ids, vec![child.id, grandchild.id]);
    }
}
