//! `delete_job_runs`: the first deletion in this codebase, and the one place a run's rows
//! disappear for good.
//!
//! The filter is the same `Select<Entity>sDataFilter` shape every select uses, built with
//! `QueryBuilder` and `WHERE 1=1` - but the five child tables carry no `status` and no `id`
//! of their own, so it is built once against `job_run` and each child delete instead
//! matches `job_run_id IN (SELECT id FROM job_run WHERE 1=1 <same filters>)`.
//!
//! Unconditional - it does not check a run's status or whether it still owes an
//! undelivered notification beyond whatever `DeleteJobRunsDataFilter` says, because those
//! guards belong to whichever caller is choosing *which* runs to delete, not to the
//! primitive that deletes them. **An entirely empty filter matches every row in `job_run`
//! and so deletes every job run in the database** - exact parity with an empty select
//! filter, which matches everything - and that is the caller's business, not this method's.

use sqlx::{Connection, SqliteConnection};

use crate::crud::CRUD;
use crate::crud::job_run::JobRunStatus;

#[derive(Debug, Clone)]
pub struct DeleteJobRunsDataFilter {
    pub id: Option<i64>,
    pub job_id: Option<String>,
    pub status: Option<JobRunStatus>,
}

#[derive(Debug, Clone)]
pub struct DeleteJobRunsData {
    pub filter: DeleteJobRunsDataFilter,
}

/// Pushes ` AND job_id = ? AND status = ? AND id = ?` for whichever fields of the filter
/// are set, onto a `... WHERE 1=1` query already open against `job_run` - shared so every
/// one of the six deletes below matches exactly the same set of runs.
fn push_job_run_filter(query_builder: &mut sqlx::QueryBuilder<sqlx::Sqlite>, filter: &DeleteJobRunsDataFilter) {

    if let Some(job_id) = &filter.job_id {
        query_builder.push(" AND job_id = ");
        query_builder.push_bind(job_id);
    }

    if let Some(status) = &filter.status {
        query_builder.push(" AND status = ");
        query_builder.push_bind(status);
    }

    if let Some(id) = &filter.id {
        query_builder.push(" AND id = ");
        query_builder.push_bind(id);
    }
}

impl CRUD {

    /// Deletes every job run matching `data.filter`, and every row across the five child
    /// tables that carries its `job_run_id`, child-first, in one transaction - so no other
    /// reader ever sees a run whose tasks are half gone.
    ///
    /// Child-first is correctness on its own terms, not the database enforcing it:
    /// `PRAGMA foreign_keys` is never set in `src/toolkit.rs`, so SQLite does not enforce
    /// the declared foreign keys at runtime here. An entirely empty
    /// `DeleteJobRunsDataFilter` deletes every job run in the database, and everything that
    /// hangs off it - see the module doc for why that is not guarded against here.
    pub async fn delete_job_runs(&self, conn: &mut SqliteConnection, data: &DeleteJobRunsData) -> anyhow::Result<()> {

        let mut tx = conn.begin().await?;

        let mut task_run_attempt_output_query: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM task_run_attempt_output WHERE job_run_id IN (SELECT id FROM job_run WHERE 1=1"
        );
        push_job_run_filter(&mut task_run_attempt_output_query, &data.filter);
        task_run_attempt_output_query.push(")");
        task_run_attempt_output_query.build().execute(&mut *tx).await?;

        let mut task_run_attempt_query: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM task_run_attempt WHERE job_run_id IN (SELECT id FROM job_run WHERE 1=1"
        );
        push_job_run_filter(&mut task_run_attempt_query, &data.filter);
        task_run_attempt_query.push(")");
        task_run_attempt_query.build().execute(&mut *tx).await?;

        let mut task_run_query: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM task_run WHERE job_run_id IN (SELECT id FROM job_run WHERE 1=1"
        );
        push_job_run_filter(&mut task_run_query, &data.filter);
        task_run_query.push(")");
        task_run_query.build().execute(&mut *tx).await?;

        let mut job_run_stop_query: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM job_run_stop WHERE job_run_id IN (SELECT id FROM job_run WHERE 1=1"
        );
        push_job_run_filter(&mut job_run_stop_query, &data.filter);
        job_run_stop_query.push(")");
        job_run_stop_query.build().execute(&mut *tx).await?;

        let mut job_run_notification_query: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM job_run_notification WHERE job_run_id IN (SELECT id FROM job_run WHERE 1=1"
        );
        push_job_run_filter(&mut job_run_notification_query, &data.filter);
        job_run_notification_query.push(")");
        job_run_notification_query.build().execute(&mut *tx).await?;

        let mut job_run_query: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM job_run WHERE 1=1"
        );
        push_job_run_filter(&mut job_run_query, &data.filter);
        job_run_query.build().execute(&mut *tx).await?;

        tx.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
    use crate::crud::job_run_notification::{NotificationChannel, NotifyOn};
    use crate::crud::multistatements::delete_job_runs::{DeleteJobRunsData, DeleteJobRunsDataFilter};
    use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRun, TaskRunStatus};
    use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttemptStatus};
    use crate::crud::task_run_attempt_output::{
        InsertTaskRunAttemptOutputData, InsertTaskRunAttemptOutputDataInput,
        SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, TaskRunAttemptOutputStream,
    };
    use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
    use crate::test_support::TestDb;

    fn delete_by_id(job_run_id: i64) -> DeleteJobRunsData {
        DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter {
                id: Some(job_run_id),
                job_id: None,
                status: None,
            },
        }
    }

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
        db.crud.delete_job_runs(&mut conn, &delete_by_id(job_run.id)).await.unwrap();

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
        db.crud.delete_job_runs(&mut conn, &delete_by_id(deleted.id)).await.unwrap();

        assert!(job_run_row(&db, deleted.id).await.is_none());

        assert!(job_run_row(&db, kept.id).await.is_some());
        assert_eq!(task_run_count(&db, kept.id).await, 1);
        assert_eq!(task_run_attempt_count(&db, kept.id).await, 1);
        assert_eq!(task_run_attempt_output_count(&db, kept.id).await, 1);
        assert_eq!(job_run_stop_count(&db, kept.id).await, 1);
        assert_eq!(db.job_run_notifications(kept.id).await.len(), 1);
    }

    /// A primitive, not a guarded operation: a filter matching nothing is a no-op rather
    /// than an error, since a caller choosing which runs to delete is the one that would
    /// ever have to explain why.
    #[tokio::test]
    async fn deleting_a_job_run_that_does_not_exist_is_a_no_op() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert!(db.crud.delete_job_runs(&mut conn, &delete_by_id(999_999)).await.is_ok());
    }

    /// The decided behaviour, pinned so a future guard cannot be added silently: an
    /// entirely empty filter matches every row in `job_run`, exactly as an empty select
    /// filter would, and so deletes every job run in the database.
    #[tokio::test]
    async fn an_empty_filter_deletes_every_job_run() {

        let db = TestDb::new().await;
        let (first, _) = full_job_run(&db).await;
        let (second, _) = full_job_run(&db).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.delete_job_runs(&mut conn, &DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter {
                id: None,
                job_id: None,
                status: None,
            },
        }).await.unwrap();

        assert!(job_run_row(&db, first.id).await.is_none());
        assert!(job_run_row(&db, second.id).await.is_none());
        assert_eq!(task_run_count(&db, first.id).await, 0);
        assert_eq!(task_run_count(&db, second.id).await, 0);
    }
}
