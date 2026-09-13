//! `delete_job_runs_with_children`: the first deletion in this codebase, and the one place a
//! run's rows disappear for good.
//!
//! The filter is resolved to matching job run ids first, with `select_job_runs` and the
//! equivalent `SelectJobRunsDataFilter` - then each of the six per-entity deletes is called
//! once per id, keyed on its own column (`job_run_id`, or `id` for `job_run` itself). That is
//! what lets every child delete stay a plain single-statement filter on one column, rather
//! than a `job_run_id IN (SELECT id FROM job_run WHERE ...)` subquery repeated six times.
//!
//! Unconditional - it does not check a run's status or whether it still owes an
//! undelivered notification beyond whatever `DeleteJobRunsDataFilter` says, because those
//! guards belong to whichever caller is choosing *which* runs to delete, not to the
//! primitive that deletes them. **An entirely empty filter matches every row in `job_run`
//! and so deletes every job run in the database** - exact parity with an empty select
//! filter, which matches everything - and that is the caller's business, not this method's.

use sqlx::{Connection, SqliteConnection};

use crate::crud::CRUD;
use crate::crud::job_run::{DeleteJobRunsData, DeleteJobRunsDataFilter, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_notification::{DeleteJobRunNotificationsData, DeleteJobRunNotificationsDataFilter};
use crate::crud::job_run_stop::{DeleteJobRunStopsData, DeleteJobRunStopsDataFilter};
use crate::crud::task_run::{DeleteTaskRunsData, DeleteTaskRunsDataFilter};
use crate::crud::task_run_attempt::{DeleteTaskRunAttemptsData, DeleteTaskRunAttemptsDataFilter};
use crate::crud::task_run_attempt_output::{DeleteTaskRunAttemptOutputsData, DeleteTaskRunAttemptOutputsDataFilter};

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
    pub async fn delete_job_runs_with_children(&self, conn: &mut SqliteConnection, data: &DeleteJobRunsData) -> anyhow::Result<()> {

        let ids = self.select_job_runs(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: data.filter.id,
                job_id: data.filter.job_id.clone(),
                status: data.filter.status,
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?
            .into_iter()
            .map(|job_run| job_run.id)
            .collect::<Vec<_>>();

        let mut tx = conn.begin().await?;

        for id in ids {

            self.delete_task_run_attempt_outputs(&mut *tx, &DeleteTaskRunAttemptOutputsData {
                filter: DeleteTaskRunAttemptOutputsDataFilter {
                    id: None,
                    task_run_attempt_id: None,
                    task_run_id: None,
                    job_run_id: Some(id),
                    job_id: None,
                    task_id: None,
                    stream: None,
                },
            }).await?;

            self.delete_task_run_attempts(&mut *tx, &DeleteTaskRunAttemptsData {
                filter: DeleteTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: Some(id),
                    task_id: None,
                    status: None,
                },
            }).await?;

            self.delete_task_runs(&mut *tx, &DeleteTaskRunsData {
                filter: DeleteTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(id),
                    job_id: None,
                    task_id: None,
                    status: None,
                },
            }).await?;

            self.delete_job_run_stops(&mut *tx, &DeleteJobRunStopsData {
                filter: DeleteJobRunStopsDataFilter {
                    id: None,
                    job_run_id: Some(id),
                },
            }).await?;

            self.delete_job_run_notifications(&mut *tx, &DeleteJobRunNotificationsData {
                filter: DeleteJobRunNotificationsDataFilter {
                    id: None,
                    job_run_id: Some(id),
                    notify_on: None,
                    channel: None,
                    status: None,
                },
            }).await?;

            self.delete_job_runs(&mut *tx, &DeleteJobRunsData {
                filter: DeleteJobRunsDataFilter {
                    id: Some(id),
                    job_id: None,
                    status: None,
                },
            }).await?;
        }

        tx.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::{DeleteJobRunsData, DeleteJobRunsDataFilter, JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
    use crate::crud::job_run_notification::{NotificationChannel, NotifyOn};
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
        db.crud.delete_job_runs_with_children(&mut conn, &delete_by_id(job_run.id)).await.unwrap();

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
        db.crud.delete_job_runs_with_children(&mut conn, &delete_by_id(deleted.id)).await.unwrap();

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

        assert!(db.crud.delete_job_runs_with_children(&mut conn, &delete_by_id(999_999)).await.is_ok());
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
        db.crud.delete_job_runs_with_children(&mut conn, &DeleteJobRunsData {
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
