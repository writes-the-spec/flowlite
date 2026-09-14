//! `delete_job_runs_with_children`: the cascade that deletes a run's rows for good, across
//! every one of the six tables a job run can own rows in.
//!
//! The filter is resolved to matching job run ids first, with `select_job_runs` and the
//! equivalent `SelectJobRunsDataFilter` - then each of the six per-entity deletes is called
//! once per id, keyed on its own column (`job_run_id`, or `id` for `job_run` itself). That is
//! what lets every child delete stay a plain single-statement filter on one column, rather
//! than each of the six having to match its rows against a subquery over `job_run` - and so
//! lets this file, like every multistatement, compose entity methods without writing a
//! statement of its own.
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
    /// The filter is resolved to ids *inside* that transaction, so the guard a caller
    /// expresses in the filter and the delete it authorises are one atomic step. The
    /// Scheduler's reconcile is the caller that needs it: it deletes only runs that are
    /// still `Submitted` and still in the future, and `JobRunReleaser` may promote such a
    /// row at any moment. Resolving the ids on the plain connection first would leave a
    /// window in which the releaser promotes a run between the check and the delete, and
    /// the reconcile would then cancel a run at the exact instant it came due, along with
    /// its task runs.
    ///
    /// Child-first is enforced by the database, not just convention: sqlx's
    /// `SqliteConnectOptions` turns `PRAGMA foreign_keys` on by default, so deleting a
    /// `job_run` before its children fails loudly with `FOREIGN KEY constraint failed`
    /// rather than leaving a silent orphan. An entirely empty `DeleteJobRunsDataFilter`
    /// deletes every job run in the database, and everything that hangs off it - see the
    /// module doc for why that is not guarded against here.
    pub async fn delete_job_runs_with_children(&self, conn: &mut SqliteConnection, data: &DeleteJobRunsData) -> anyhow::Result<()> {

        let mut tx = conn.begin().await?;

        // `scheduled_at_gt` has no counterpart on `SelectJobRunsDataFilter` - see the module
        // doc for why the two filter types must otherwise stay in lockstep - so it is applied
        // below, in Rust, over the rows this select resolves.
        let ids = self.select_job_runs(&mut *tx, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: data.filter.id,
                job_id: data.filter.job_id.clone(),
                status: data.filter.status,
                statuses: None,
                scheduled_at_lte: None,
                schedule_id: data.filter.schedule_id.clone(),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?
            .into_iter()
            .filter(|job_run| match data.filter.scheduled_at_gt {
                Some(after) => job_run.scheduled_at > after,
                None => true,
            })
            .map(|job_run| job_run.id)
            .collect::<Vec<_>>();

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
                    schedule_id: None,
                    scheduled_at_gt: None,
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

    /// Offsets every child table's own autoincrement id by a different amount before a
    /// test builds its `full_job_run`s, so that within one job run's six rows, no two of
    /// `job_run.id`, `task_run.id`, `task_run_attempt.id`, `task_run_attempt_output.id`,
    /// `job_run_stop.id` and `job_run_notification.id` ever coincide.
    ///
    /// Without this, a fixture built from exactly one row per table advances every one of
    /// those six sequences in lockstep - `full_job_run` called twice gives ids (1,1,1,1,1,1)
    /// and (2,2,2,2,2,2) - so a child delete that filtered on the wrong column (its own
    /// `id`, or a sibling table's id) would still happen to hit the right row, and neither
    /// cascade test below would notice.
    async fn prime_distinct_child_ids(db: &TestDb) {

        let padding_job_run = db.insert_job_run(JobRunStatus::Succeeded).await;

        db.insert_task_run(padding_job_run.id, TaskRunStatus::Succeeded).await;
        let padding_task_run = db.insert_task_run(padding_job_run.id, TaskRunStatus::Succeeded).await;

        let mut padding_attempt = db.insert_task_run_attempt(&padding_task_run, 1, TaskRunAttemptStatus::Succeeded).await;
        for attempt_number in 2..=3 {
            padding_attempt = db.insert_task_run_attempt(&padding_task_run, attempt_number, TaskRunAttemptStatus::Succeeded).await;
        }

        for _ in 0..4 {
            db.crud.insert_task_run_attempt_output(
                &*db.conn_pool,
                &InsertTaskRunAttemptOutputData {
                    input: InsertTaskRunAttemptOutputDataInput {
                        task_run_attempt_id: padding_attempt.id,
                        task_run_id: padding_task_run.id,
                        job_run_id: padding_job_run.id,
                        job_id: padding_task_run.job_id.clone(),
                        task_id: padding_task_run.task_id.clone(),
                        stream: TaskRunAttemptOutputStream::Stdout,
                        content: "padding".to_string(),
                    },
                },
            ).await.unwrap();
        }

        for _ in 0..5 {
            db.insert_job_run_stop(padding_job_run.id).await;
        }

        for _ in 0..6 {
            db.insert_job_run_notification(
                padding_job_run.id,
                NotifyOn::Failure,
                NotificationChannel::Email,
                &["oncall@example.com"],
            ).await;
        }
    }

    fn delete_by_id(job_run_id: i64) -> DeleteJobRunsData {
        DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter {
                id: Some(job_run_id),
                job_id: None,
                status: None,
                schedule_id: None,
                scheduled_at_gt: None,
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
                statuses: None,
                scheduled_at_lte: None,
                schedule_id: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await.unwrap()
    }

    /// Every row a run can own - across all six tables - is gone once it is deleted, and
    /// the run itself with them.
    ///
    /// `prime_distinct_child_ids` keeps `job_run.id`, `task_run.id`, `task_run_attempt.id`,
    /// `task_run_attempt_output.id`, `job_run_stop.id` and `job_run_notification.id` from
    /// coinciding, so a child delete that filtered on the wrong column would leave a
    /// nonzero count here rather than passing by coincidence.
    #[tokio::test]
    async fn deleting_a_job_run_removes_its_rows_from_all_six_tables() {

        let db = TestDb::new().await;
        prime_distinct_child_ids(&db).await;
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
    ///
    /// `prime_distinct_child_ids` keeps every child table's id from coinciding with
    /// `job_run.id` - see its doc comment.
    #[tokio::test]
    async fn a_neighbouring_runs_rows_survive_deletion() {

        let db = TestDb::new().await;
        prime_distinct_child_ids(&db).await;
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
                schedule_id: None,
                scheduled_at_gt: None,
            },
        }).await.unwrap();

        assert!(job_run_row(&db, first.id).await.is_none());
        assert!(job_run_row(&db, second.id).await.is_none());
        assert_eq!(task_run_count(&db, first.id).await, 0);
        assert_eq!(task_run_count(&db, second.id).await, 0);
    }
}
