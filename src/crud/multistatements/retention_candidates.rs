//! Which finished job runs may be deleted — one operation spanning `job_run` and
//! `job_run_notification`, so it belongs to neither entity file. Every query here is
//! read-only; `delete_job_runs` is the only place a run's rows actually disappear.
//!
//! None of the three methods decides *how many* to delete, or resolves a job's own
//! `keep_runs` — that policy belongs to the retention service. They only answer, for a
//! given shape of question, which finished runs are candidates right now.

use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::JobRunStatus;
use crate::crud::job_run_notification::JobRunNotificationStatus;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SelectDeletableJobRunsDataSort {
    NewestFirst,
    OldestFirst,
}

#[derive(Debug, Clone)]
pub struct SelectDeletableJobRunsDataFilter {
    pub job_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SelectDeletableJobRunsData {
    pub filter: SelectDeletableJobRunsDataFilter,
    pub sort: Option<SelectDeletableJobRunsDataSort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl CRUD {

    /// How many job runs across every job have settled — what the global `keep_runs_total`
    /// ceiling is compared against.
    pub async fn count_finished_job_runs(&self, conn: &mut SqliteConnection) -> anyhow::Result<u32> {

        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT COUNT(*) FROM job_run WHERE status IN "
        );
        push_finished_statuses(&mut query_builder);

        let count = query_builder
            .build_query_scalar::<i64>()
            .fetch_one(&mut *conn)
            .await?;

        Ok(count as u32)
    }

    /// Every job id that owns at least one finished run, including one with no row in
    /// `mem.job` at all: an ad-hoc definition, or a job whose YAML has since been deleted.
    /// It is what the per-job pass iterates over, since nothing else enumerates "jobs that
    /// have ever run" the way `mem.job` enumerates "jobs currently declared".
    pub async fn select_job_ids_with_finished_job_runs(&self, conn: &mut SqliteConnection) -> anyhow::Result<Vec<String>> {

        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT DISTINCT job_id FROM job_run WHERE status IN "
        );
        push_finished_statuses(&mut query_builder);

        let job_ids = query_builder
            .build_query_scalar::<String>()
            .fetch_all(&mut *conn)
            .await?;

        Ok(job_ids)
    }

    /// Finished runs matching `data.filter`, minus any that still owe a `Pending`
    /// notification, ranked and windowed by `data.sort`/`data.offset`, and capped at
    /// `data.limit`.
    ///
    /// `offset` **is** `keep_runs`: `sort: NewestFirst` with `offset: Some(keep_runs)` is
    /// the per-job window ("every finished run of this job past the newest `keep_runs`"),
    /// and `sort: OldestFirst` with `offset: None` is the global oldest-first sweep across
    /// every matched job. They are the same query because `offset` and `sort` are exactly
    /// what distinguish them.
    ///
    /// **`offset: None` and `offset: Some(0)` both mean no offset** — the inverse of what
    /// `0` means for every other number in this plan, where `0` means "no limit". With no
    /// offset (and no `job_id` filter), every finished run of the matched jobs is a
    /// candidate; deleting all of them is irreversible.
    ///
    /// `limit: None` means no `LIMIT` clause at all, since SQLite's `LIMIT 0` means zero
    /// rows rather than unlimited.
    ///
    /// The rank is computed by the inner subquery, before the notification filter is
    /// applied by the outer one, on purpose: a run awaiting delivery still occupies its
    /// ranked slot, so excluding it must not pull a neighbour across the `offset` line to
    /// compensate. `LIMIT -1 OFFSET <offset>` is SQLite's way of saying "every row past the
    /// first `offset`, in this order" without a window function.
    pub async fn select_deletable_job_runs(&self, conn: &mut SqliteConnection, data: &SelectDeletableJobRunsData) -> anyhow::Result<Vec<i64>> {

        let order_by = data.sort.map(|sort| match sort {
            SelectDeletableJobRunsDataSort::NewestFirst => " ORDER BY id DESC",
            SelectDeletableJobRunsDataSort::OldestFirst => " ORDER BY id ASC",
        });

        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id FROM (SELECT id FROM job_run WHERE 1=1"
        );

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        query_builder.push(" AND status IN ");
        push_finished_statuses(&mut query_builder);

        if let Some(order_by) = order_by {
            query_builder.push(order_by);
        }

        if let Some(offset) = data.offset {
            query_builder.push(" LIMIT -1 OFFSET ");
            query_builder.push_bind(offset as i64);
        }

        query_builder.push(
            ") r WHERE NOT EXISTS (SELECT 1 FROM job_run_notification n WHERE n.job_run_id = r.id AND n.status = "
        );
        query_builder.push_bind(JobRunNotificationStatus::Pending);
        query_builder.push(")");

        if let Some(order_by) = order_by {
            query_builder.push(order_by);
        }

        if let Some(limit) = data.limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit as i64);
        }

        let ids = query_builder
            .build_query_scalar::<i64>()
            .fetch_all(&mut *conn)
            .await?;

        Ok(ids)
    }
}

/// Pushes `IN (?, ?, ...)`, bound to every status `JobRunStatus::is_finished` calls true.
/// Built from that exhaustive match rather than written out here, so a status added later
/// has to say which side of it falls on before this query compiles at all.
fn push_finished_statuses(query_builder: &mut sqlx::QueryBuilder<sqlx::Sqlite>) {

    query_builder.push("(");

    let mut separated = query_builder.separated(", ");
    for status in JobRunStatus::ALL.iter().filter(|status| status.is_finished()) {
        separated.push_bind(*status);
    }

    query_builder.push(")");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRun, JobRunStatus};
    use crate::crud::job_run_notification::{NotificationChannel, NotifyOn};
    use crate::crud::multistatements::retention_candidates::{
        SelectDeletableJobRunsData, SelectDeletableJobRunsDataFilter, SelectDeletableJobRunsDataSort,
    };
    use crate::test_support::TestDb;

    /// A finished run under a caller-chosen job id, for the tests that need more than one
    /// job — `TestDb::insert_job_run` always writes `job_id: "job"`.
    async fn insert_finished_run_for(db: &TestDb, job_id: &str, status: JobRunStatus) -> JobRun {

        let id = db.crud.insert_job_run(
            &*db.conn_pool,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: job_id.to_string(),
                    job_name: "Job".to_string(),
                    job_description: String::new(),
                    parameters: BTreeMap::new(),
                    scheduled_at: None,
                    status,
                },
            },
        ).await.unwrap();

        db.job_run(id).await
    }

    fn ids(runs: &[JobRun]) -> Vec<i64> {
        runs.iter().map(|run| run.id).collect()
    }

    /// Per-job window: `sort: NewestFirst` with `offset: Some(keep_runs)` — five finished
    /// runs, `keep_runs = 3`: only the two oldest are candidates, whatever order the query
    /// happens to return them in.
    #[tokio::test]
    async fn newest_first_with_an_offset_keeps_the_newest_n() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let mut candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            sort: Some(SelectDeletableJobRunsDataSort::NewestFirst),
            limit: None,
            offset: Some(3),
        }).await.unwrap();
        candidates.sort();

        let mut expected = ids(&runs[0..2]);
        expected.sort();

        assert_eq!(candidates, expected);
    }

    /// The run at rank position `keep_runs` — the oldest run *inside* the kept window, not
    /// the globally oldest run — still counts toward that window even though a `Pending`
    /// notification keeps it out of the result, so excluding it must not pull the next run
    /// into the result to compensate. Filtering before ranking would do exactly that: with
    /// the notification on `runs[2]` (rank 3 of 5, `keep_runs = 3`), a filter-then-rank
    /// query would re-rank the surviving four runs and wrongly protect `runs[1]` (which
    /// would shift from rank 4 to rank 3); this is the regression that rules it out.
    #[tokio::test]
    async fn a_pending_notification_excludes_a_run_without_shifting_the_others_rank() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        db.insert_job_run_notification(runs[2].id, NotifyOn::Failure, NotificationChannel::Email, &["oncall@example.com"]).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let mut candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            sort: Some(SelectDeletableJobRunsDataSort::NewestFirst),
            limit: None,
            offset: Some(3),
        }).await.unwrap();
        candidates.sort();

        assert_eq!(candidates, vec![runs[0].id, runs[1].id]);
    }

    /// A `Sent` notification is done owing anything, so the run behind it is a candidate
    /// again — only a `Pending` row excludes a run.
    #[tokio::test]
    async fn a_sent_notification_does_not_exclude_its_run() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let notification = db.insert_job_run_notification(runs[0].id, NotifyOn::Failure, NotificationChannel::Email, &["oncall@example.com"]).await;

        sqlx::query("UPDATE job_run_notification SET status = 'sent' WHERE id = ?")
            .bind(notification.id)
            .execute(&*db.conn_pool)
            .await
            .unwrap();

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let mut candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            sort: Some(SelectDeletableJobRunsDataSort::NewestFirst),
            limit: None,
            offset: Some(3),
        }).await.unwrap();
        candidates.sort();

        assert_eq!(candidates, vec![runs[0].id, runs[1].id]);
    }

    /// `Pending` and `Running` runs are never candidates, even when they are the oldest
    /// rows and an offset of `0` would otherwise put them past the kept window.
    #[tokio::test]
    async fn pending_and_running_runs_are_never_returned() {

        let db = TestDb::new().await;

        let pending = db.insert_job_run(JobRunStatus::Pending).await;
        let running = db.insert_job_run(JobRunStatus::Running).await;
        let finished = db.insert_job_run(JobRunStatus::Failed).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            sort: Some(SelectDeletableJobRunsDataSort::NewestFirst),
            limit: None,
            offset: None,
        }).await.unwrap();

        assert!(!candidates.contains(&pending.id));
        assert!(!candidates.contains(&running.id));
        assert_eq!(candidates, vec![finished.id]);
    }

    /// `limit` caps how many candidates come back — `None` means uncapped.
    #[tokio::test]
    async fn select_deletable_job_runs_respects_its_limit() {

        let db = TestDb::new().await;

        for _ in 0..5 {
            db.insert_job_run(JobRunStatus::Succeeded).await;
        }

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            sort: Some(SelectDeletableJobRunsDataSort::NewestFirst),
            limit: Some(2),
            offset: None,
        }).await.unwrap();

        assert_eq!(candidates.len(), 2);
    }

    /// Global sweep: `sort: OldestFirst` with `offset: None` and no `job_id` filter crosses
    /// jobs rather than partitioning by one, oldest first, truncated at `limit`.
    #[tokio::test]
    async fn oldest_first_crosses_jobs_and_respects_its_limit() {

        let db = TestDb::new().await;

        let a1 = insert_finished_run_for(&db, "job-a", JobRunStatus::Succeeded).await;
        let b1 = insert_finished_run_for(&db, "job-b", JobRunStatus::Succeeded).await;
        let a2 = insert_finished_run_for(&db, "job-a", JobRunStatus::Succeeded).await;
        let _b2 = insert_finished_run_for(&db, "job-b", JobRunStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let oldest = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: None },
            sort: Some(SelectDeletableJobRunsDataSort::OldestFirst),
            limit: Some(3),
            offset: None,
        }).await.unwrap();

        assert_eq!(oldest, vec![a1.id, b1.id, a2.id]);
    }

    /// `Pending`, `Running` and notification-owing runs are excluded here exactly as they
    /// are from the per-job window.
    #[tokio::test]
    async fn oldest_first_excludes_unfinished_and_pending_notification_runs() {

        let db = TestDb::new().await;

        let running = db.insert_job_run(JobRunStatus::Running).await;
        let owed = db.insert_job_run(JobRunStatus::Failed).await;
        db.insert_job_run_notification(owed.id, NotifyOn::Failure, NotificationChannel::Email, &["oncall@example.com"]).await;
        let deletable = db.insert_job_run(JobRunStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let oldest = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: None },
            sort: Some(SelectDeletableJobRunsDataSort::OldestFirst),
            limit: None,
            offset: None,
        }).await.unwrap();

        assert_eq!(oldest, vec![deletable.id]);
        assert!(!oldest.contains(&running.id));
        assert!(!oldest.contains(&owed.id));
    }

    /// `sort` really does change which end of the history comes back, on the very same
    /// fixture: `NewestFirst` with `limit: Some(1)` returns the newest finished run,
    /// `OldestFirst` returns the oldest.
    #[tokio::test]
    async fn newest_first_and_oldest_first_return_different_ends_of_the_same_fixture() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let mut conn = db.conn_pool.acquire().await.unwrap();

        let newest = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            sort: Some(SelectDeletableJobRunsDataSort::NewestFirst),
            limit: Some(1),
            offset: None,
        }).await.unwrap();

        let oldest = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            sort: Some(SelectDeletableJobRunsDataSort::OldestFirst),
            limit: Some(1),
            offset: None,
        }).await.unwrap();

        assert_eq!(newest, vec![runs[4].id]);
        assert_eq!(oldest, vec![runs[0].id]);
    }

    /// Counts settled runs across every job, and leaves `Pending`/`Running` out — what the
    /// global `keep_runs_total` ceiling is compared against.
    #[tokio::test]
    async fn count_finished_job_runs_counts_only_settled_runs() {

        let db = TestDb::new().await;

        db.insert_job_run(JobRunStatus::Pending).await;
        db.insert_job_run(JobRunStatus::Running).await;
        insert_finished_run_for(&db, "job-a", JobRunStatus::Succeeded).await;
        insert_finished_run_for(&db, "job-b", JobRunStatus::Failed).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let count = db.crud.count_finished_job_runs(&mut conn).await.unwrap();

        assert_eq!(count, 2);
    }

    /// Every job id owning a finished run comes back, including one with no `mem.job` row
    /// at all — `TestDb` leaves `mem` unmigrated, so this also proves the query never joins
    /// against it.
    #[tokio::test]
    async fn select_job_ids_with_finished_job_runs_includes_ids_with_no_job_row() {

        let db = TestDb::new().await;

        db.insert_job_run(JobRunStatus::Running).await;
        insert_finished_run_for(&db, "ad-hoc-job", JobRunStatus::Succeeded).await;
        insert_finished_run_for(&db, "deleted-job", JobRunStatus::Failed).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let mut job_ids = db.crud.select_job_ids_with_finished_job_runs(&mut conn).await.unwrap();
        job_ids.sort();

        assert_eq!(job_ids, vec!["ad-hoc-job".to_string(), "deleted-job".to_string()]);
    }
}
