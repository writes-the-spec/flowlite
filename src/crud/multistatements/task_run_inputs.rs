//! What one job run's tasks have produced so far, as the attempt numbers their results
//! were written under - the other half of `FLOWLITE_TASK_OUTPUT`, read when a dependent
//! task's command is about to be spawned.

use std::collections::BTreeMap;
use sqlx::SqliteConnection;

use serde::Serialize;

use crate::crud::CRUD;
use crate::crud::task_run_attempt::{
    SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttemptStatus,
};

/// One task's result: what the attempt that succeeded wrote, and which attempt that was.
///
/// The attempt number is what names the file on disk, so a dependent can be pointed at it;
/// the content is what a reader of the run wants, so nothing has to go back to the
/// filesystem to answer "what did this run produce".
#[derive(Debug, Clone, Serialize)]
pub struct TaskRunOutput {
    pub attempt: u32,
    pub content: String,
}

impl CRUD {

    /// The result of each task of one job run that produced one, keyed by task id.
    ///
    /// Only a `Succeeded` attempt is here, which is what makes the task's result "what the
    /// attempt that worked produced" rather than whatever the last attempt left behind. A
    /// task that wrote nothing has no entry at all, so a dependent is given no variable for
    /// it rather than a path to a file that is not there.
    ///
    /// One query for the whole run rather than one per dependency: a task with four
    /// dependencies is one select, and a run's attempts are already indexed by job_run_id.
    /// The caller picks out the task ids it actually depends on.
    pub async fn select_task_run_outputs(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
    ) -> anyhow::Result<BTreeMap<String, TaskRunOutput>> {

        let attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: Some(job_run_id),
                task_id: None,
                status: Some(TaskRunAttemptStatus::Succeeded),
            },
            sort: None,
        }).await?;

        let mut outputs = BTreeMap::new();

        for attempt in attempts {
            if !attempt.output.is_empty() {
                outputs.insert(attempt.task_id, TaskRunOutput {
                    attempt: attempt.attempt,
                    content: attempt.output,
                });
            }
        }

        Ok(outputs)
    }
}


#[cfg(test)]
mod tests {
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::{
        TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter,
        UpdateTaskRunAttemptsDataInput,
    };
    use crate::crud::job_run::JobRunStatus;
    use crate::test_support::TestDb;

    async fn attempt_with_output(
        db: &TestDb,
        job_run_id: i64,
        task_id: &str,
        attempt: u32,
        status: TaskRunAttemptStatus,
        output: &str,
    ) {
        let task_run = db.insert_named_task_run(job_run_id, task_id, TaskRunStatus::Succeeded).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, attempt, status).await;

        db.crud.update_task_run_attempts(
            &*db.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: None,
                    started_at: None,
                    finished_at: None,
                    process_group_id: None,
                    output: Some(output.to_string()),
                },
            },
        ).await.unwrap();
    }

    #[tokio::test]
    async fn a_succeeded_attempts_output_is_reported_under_its_task_id() {
        let db = TestDb::new().await;
        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        attempt_with_output(&db, job_run.id, "plan", 1, TaskRunAttemptStatus::Succeeded, "the plan").await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let outputs = db.crud.select_task_run_outputs(&mut conn, job_run.id).await.unwrap();

        let output = outputs.get("plan").unwrap();

        assert_eq!(output.attempt, 1);
        assert_eq!(output.content, "the plan");
    }

    /// A task that produced nothing is absent rather than present-and-empty: the caller
    /// turns this map into environment variables, and an absent entry is what lets a
    /// dependent ask whether it got a result at all.
    #[tokio::test]
    async fn a_succeeded_attempt_that_wrote_nothing_is_absent() {
        let db = TestDb::new().await;
        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        attempt_with_output(&db, job_run.id, "plan", 1, TaskRunAttemptStatus::Succeeded, "").await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let outputs = db.crud.select_task_run_outputs(&mut conn, job_run.id).await.unwrap();

        assert!(outputs.is_empty(), "{outputs:?}");
    }

    /// The result is the succeeding attempt's, not the last attempt's. A failed attempt's
    /// file stays on disk for whoever is reading the run back, and is not what a dependent
    /// is pointed at.
    #[tokio::test]
    async fn a_failed_attempts_output_is_not_the_tasks_result() {
        let db = TestDb::new().await;
        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        attempt_with_output(&db, job_run.id, "plan", 1, TaskRunAttemptStatus::Failed, "a wrong plan").await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let outputs = db.crud.select_task_run_outputs(&mut conn, job_run.id).await.unwrap();

        assert!(outputs.is_empty(), "{outputs:?}");
    }

    #[tokio::test]
    async fn another_runs_output_is_not_reported() {
        let db = TestDb::new().await;
        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let other_job_run = db.insert_job_run(JobRunStatus::Running).await;

        attempt_with_output(&db, other_job_run.id, "plan", 1, TaskRunAttemptStatus::Succeeded, "the plan").await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let outputs = db.crud.select_task_run_outputs(&mut conn, job_run.id).await.unwrap();

        assert!(outputs.is_empty(), "{outputs:?}");
    }
}
