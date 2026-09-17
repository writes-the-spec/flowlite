//! A task's own process saying it has begun, or stopped, waiting on another run.
//!
//! Two statements rather than one update, because the write is conditional on what the row
//! says: the caller is a process holding an attempt id out of its environment, and that id
//! can name an attempt that has since settled - a backgrounded wait outliving the task that
//! started it. An unconditional update would let such a process free a slot on a finished
//! row. Reading first is the whole reason this is not a plain entity update.

use chrono::Utc;
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::task_run_attempt::{
    SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttemptStatus,
    UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput,
};

impl CRUD {

    /// Stamps the attempt as waiting, and answers whether it did. Only a Running attempt is
    /// stamped: one that is queued has no process to be waiting, and one that has settled is
    /// no longer this caller's to speak for. An id matching no row answers `false` rather
    /// than raising - the caller is a wait loop inside somebody's task, and a bookkeeping
    /// miss must not fail their pipeline.
    pub async fn mark_attempt_waiting(
        &self,
        conn: &mut SqliteConnection,
        task_run_attempt_id: i64,
    ) -> anyhow::Result<bool> {

        let attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: None,
            },
            sort: None,
        }).await?;

        let Some(attempt) = attempts.into_iter().find(|attempt| attempt.id == task_run_attempt_id) else {
            return Ok(false);
        };

        if attempt.status != TaskRunAttemptStatus::Running {
            return Ok(false);
        }

        self.update_task_run_attempts(&mut *conn, &UpdateTaskRunAttemptsData {
            input: UpdateTaskRunAttemptsDataInput {
                status: None,
                started_at: None,
                finished_at: None,
                process_group_id: None,
                output: None,
                waiting_since: Some(Some(Utc::now())),
            },
            filter: UpdateTaskRunAttemptsDataFilter { id: Some(task_run_attempt_id), task_run_id: None },
        }).await?;

        Ok(true)
    }

    /// Removes the stamp, whatever the attempt's status now is. Unconditional where marking
    /// is conditional: this runs as a wait unwinds, by which time the monitor may already
    /// have settled the attempt, and refusing then would leave the stamp behind to no
    /// purpose. Writing to a row that is no longer Running changes nothing any tally reads.
    pub async fn clear_attempt_waiting(
        &self,
        conn: &mut SqliteConnection,
        task_run_attempt_id: i64,
    ) -> anyhow::Result<()> {

        self.update_task_run_attempts(&mut *conn, &UpdateTaskRunAttemptsData {
            input: UpdateTaskRunAttemptsDataInput {
                status: None,
                started_at: None,
                finished_at: None,
                process_group_id: None,
                output: None,
                waiting_since: Some(None),
            },
            filter: UpdateTaskRunAttemptsDataFilter { id: Some(task_run_attempt_id), task_run_id: None },
        }).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::test_support::TestDb;

    /// The ordinary path: a task's own process says it has started waiting, and the row
    /// carries an instant from then on.
    #[tokio::test]
    async fn marking_a_running_attempt_stamps_it() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let marked = db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        assert!(marked);
        assert!(db.last_task_run_attempt(task_run.id).await.waiting_since.is_some());
    }

    /// The reason this is a multistatement rather than one update: a process can outlive
    /// the attempt whose id it inherited - a backgrounded `job-run wait` still polling
    /// after its parent task settled - and must not be able to free a slot on a row that
    /// finished. Reading first is what refuses it.
    #[tokio::test]
    async fn marking_a_settled_attempt_writes_nothing() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let marked = db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        assert!(!marked);
        assert_eq!(db.last_task_run_attempt(task_run.id).await.waiting_since, None);
    }

    /// A queued attempt has no process, so nothing of it can be waiting.
    #[tokio::test]
    async fn marking_a_queued_attempt_writes_nothing() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Queued).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert!(!db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap());
    }

    /// An id naming no row at all - the shape a deleted run leaves behind - is not an
    /// error. The caller is a wait loop in somebody's task; failing it would turn a
    /// bookkeeping miss into a failed pipeline.
    #[tokio::test]
    async fn marking_an_attempt_that_does_not_exist_is_not_an_error() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert!(!db.crud.mark_attempt_waiting(&mut conn, 404).await.unwrap());
    }

    #[tokio::test]
    async fn clearing_removes_the_stamp() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();
        db.crud.clear_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        assert_eq!(db.last_task_run_attempt(task_run.id).await.waiting_since, None);
    }

    /// Clearing is unconditional on status, unlike marking: the guard that calls it runs
    /// as the wait unwinds, by which time the attempt may well have been settled by the
    /// monitor, and refusing then would leave the stamp behind for no reason.
    #[tokio::test]
    async fn clearing_a_settled_attempt_still_removes_the_stamp() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        db.settle_task_run_attempt(attempt.id, TaskRunAttemptStatus::TimedOut).await;

        db.crud.clear_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        assert_eq!(db.last_task_run_attempt(task_run.id).await.waiting_since, None);
    }
}
