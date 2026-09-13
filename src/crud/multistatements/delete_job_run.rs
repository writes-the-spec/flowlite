//! `delete_job_run`: the first deletion in this codebase, and the one place a run's rows
//! disappear for good.
//!
//! Unconditional - it does not check the run's status or whether it still owes an
//! undelivered notification, because those guards belong to whichever caller is choosing
//! *which* runs to delete, not to the primitive that deletes one. A deleter that silently
//! refuses a run at hand would be a worse tool for a later, deliberate caller (a
//! `job-run delete` command, say).

use sqlx::{Connection, SqliteConnection};

use crate::crud::CRUD;

impl CRUD {

    /// Deletes one job run and every row across the six tables that carries its
    /// `job_run_id`, child-first, in one transaction - so no other reader ever sees a run
    /// whose tasks are half gone.
    ///
    /// Child-first is correctness on its own terms, not the database enforcing it:
    /// `PRAGMA foreign_keys` is never set in `src/toolkit.rs`, so SQLite does not enforce
    /// the declared foreign keys at runtime here. A `job_run_id` with no matching row in
    /// any of the six tables is a no-op, not an error - this is a primitive, and whether
    /// that should ever happen is a caller's question.
    pub async fn delete_job_run(&self, conn: &mut SqliteConnection, job_run_id: i64) -> anyhow::Result<()> {

        let mut tx = conn.begin().await?;

        sqlx::query("DELETE FROM task_run_attempt_output WHERE job_run_id = ?")
            .bind(job_run_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM task_run_attempt WHERE job_run_id = ?")
            .bind(job_run_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM task_run WHERE job_run_id = ?")
            .bind(job_run_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM job_run_stop WHERE job_run_id = ?")
            .bind(job_run_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM job_run_notification WHERE job_run_id = ?")
            .bind(job_run_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM job_run WHERE id = ?")
            .bind(job_run_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
    use crate::crud::job_run_notification::{NotificationChannel, NotifyOn};
    use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRun, TaskRunStatus};
    use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttemptStatus};
    use crate::crud::task_run_attempt_output::{
        InsertTaskRunAttemptOutputData, InsertTaskRunAttemptOutputDataInput,
        SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, TaskRunAttemptOutputStream,
    };
    use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
    use crate::test_support::TestDb;

    /// One job run carrying one row in every one of the six tables, built the way a real
    /// run accumulates them: a task run, an attempt of it, output from that attempt, a
    /// stop request, and an open notification.
    async fn full_job_run(db: &TestDb) -> (JobRun, TaskRun) {

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Failed).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Failed).await;

        db.crud.insert_task_run_attempt_output(
            &*db.conn_pool,
            &InsertTaskRunAttemptOutputData {
                input: InsertTaskRunAttemptOutputDataInput {
                    task_run_attempt_id: task_run_attempt.id,
                    task_run_id: task_run.id,
                    job_run_id: job_run.id,
                    job_id: task_run.job_id.clone(),
                    task_id: task_run.task_id.clone(),
                    stream: TaskRunAttemptOutputStream::Stdout,
                    content: "boom".to_string(),
                },
            },
        ).await.unwrap();

        db.insert_job_run_stop(job_run.id).await;

        db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Failure,
            NotificationChannel::Email,
            &["oncall@example.com"],
        ).await;

        (job_run, task_run)
    }

    async fn task_run_count(db: &TestDb, job_run_id: i64) -> usize {
        db.crud.select_task_runs(&*db.conn_pool, &SelectTaskRunsData {
            filter: SelectTaskRunsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
                job_id: None,
                task_id: None,
                status: None,
            },
            sort: None,
        }).await.unwrap().len()
    }

    async fn task_run_attempt_count(db: &TestDb, job_run_id: i64) -> usize {
        db.crud.select_task_run_attempts(&*db.conn_pool, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: Some(job_run_id),
                task_id: None,
                status: None,
            },
            sort: None,
        }).await.unwrap().len()
    }

    async fn task_run_attempt_output_count(db: &TestDb, job_run_id: i64) -> usize {
        db.crud.select_task_run_attempt_outputs(&*db.conn_pool, &SelectTaskRunAttemptOutputsData {
            filter: SelectTaskRunAttemptOutputsDataFilter {
                id: None,
                task_run_attempt_id: None,
                task_run_id: None,
                job_run_id: Some(job_run_id),
                job_id: None,
                task_id: None,
                stream: None,
            },
            sort: None,
        }).await.unwrap().len()
    }

    async fn job_run_stop_count(db: &TestDb, job_run_id: i64) -> usize {
        db.crud.select_job_run_stops(&*db.conn_pool, &SelectJobRunStopsData {
            filter: SelectJobRunStopsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap().len()
    }

    async fn job_run_row(db: &TestDb, job_run_id: i64) -> Option<JobRun> {
        db.crud.select_job_run(&*db.conn_pool, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: Some(job_run_id),
                job_id: None,
                status: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await.unwrap()
    }

    /// Every row a run can own - across all six tables - is gone once it is deleted, and
    /// the run itself with them.
    #[tokio::test]
    async fn deleting_a_job_run_removes_its_rows_from_all_six_tables() {

        let db = TestDb::new().await;
        let (job_run, _task_run) = full_job_run(&db).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.delete_job_run(&mut conn, job_run.id).await.unwrap();

        assert!(job_run_row(&db, job_run.id).await.is_none());
        assert_eq!(task_run_count(&db, job_run.id).await, 0);
        assert_eq!(task_run_attempt_count(&db, job_run.id).await, 0);
        assert_eq!(task_run_attempt_output_count(&db, job_run.id).await, 0);
        assert_eq!(job_run_stop_count(&db, job_run.id).await, 0);
        assert_eq!(db.job_run_notifications(job_run.id).await.len(), 0);
    }

    /// The whole point of keying every statement on `job_run_id` (or, for `job_run`
    /// itself, `id`): a neighbouring run's rows in the very same six tables are left
    /// completely untouched.
    #[tokio::test]
    async fn a_neighbouring_runs_rows_survive_deletion() {

        let db = TestDb::new().await;
        let (deleted, _) = full_job_run(&db).await;
        let (kept, _) = full_job_run(&db).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.delete_job_run(&mut conn, deleted.id).await.unwrap();

        assert!(job_run_row(&db, deleted.id).await.is_none());

        assert!(job_run_row(&db, kept.id).await.is_some());
        assert_eq!(task_run_count(&db, kept.id).await, 1);
        assert_eq!(task_run_attempt_count(&db, kept.id).await, 1);
        assert_eq!(task_run_attempt_output_count(&db, kept.id).await, 1);
        assert_eq!(job_run_stop_count(&db, kept.id).await, 1);
        assert_eq!(db.job_run_notifications(kept.id).await.len(), 1);
    }

    /// A primitive, not a guarded operation: an id nothing owns is a no-op rather than an
    /// error, since task 5's selection is the caller that would ever have to explain why.
    #[tokio::test]
    async fn deleting_a_job_run_that_does_not_exist_is_a_no_op() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert!(db.crud.delete_job_run(&mut conn, 999_999).await.is_ok());
    }
}
