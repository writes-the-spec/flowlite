//! Which finished job runs may be deleted — one operation spanning `job_run` and
//! `job_run_notification`, so it belongs to neither entity file. Every query here is
//! read-only; `delete_job_run` is the only place a run's rows actually disappear.
//!
//! None of the four methods decides *how many* to delete, or resolves a job's own
//! `keep_runs` — that policy belongs to the retention service. These only answer, for a
//! given shape of question, which finished runs are candidates right now.

use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::JobRunStatus;
use crate::crud::job_run_notification::JobRunNotificationStatus;

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

    /// This job's finished runs past its newest `keep_runs`, minus any that still owe a
    /// `Pending` notification — capped at `limit` (0 for no cap).
    ///
    /// The rank is computed by the inner subquery, before the notification filter is
    /// applied by the outer one, on purpose: a run awaiting delivery still occupies one of
    /// the newest `keep_runs` slots, so excluding it must not pull an older neighbour into
    /// the window. `LIMIT -1 OFFSET keep_runs` is SQLite's way of saying "every row past
    /// the newest `keep_runs`" without a window function.
    pub async fn select_deletable_job_runs_for_job(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
        keep_runs: u32,
        limit: u32,
    ) -> anyhow::Result<Vec<i64>> {

        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id FROM (SELECT id FROM job_run WHERE job_id = "
        );
        query_builder.push_bind(job_id);
        query_builder.push(" AND status IN ");
        push_finished_statuses(&mut query_builder);
        query_builder.push(" ORDER BY id DESC LIMIT -1 OFFSET ");
        query_builder.push_bind(keep_runs as i64);
        query_builder.push(
            ") r WHERE NOT EXISTS (SELECT 1 FROM job_run_notification n WHERE n.job_run_id = r.id AND n.status = "
        );
        query_builder.push_bind(JobRunNotificationStatus::Pending);
        query_builder.push(")");

        if limit != 0 {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit as i64);
        }

        let ids = query_builder
            .build_query_scalar::<i64>()
            .fetch_all(&mut *conn)
            .await?;

        Ok(ids)
    }

    /// Finished runs owing no `Pending` notification, oldest first across every job —
    /// what the global `keep_runs_total` ceiling deletes from, since it is enforced
    /// without regard to which job a run belongs to. Capped at `limit` (0 for no cap).
    pub async fn select_oldest_deletable_job_runs(
        &self,
        conn: &mut SqliteConnection,
        limit: u32,
    ) -> anyhow::Result<Vec<i64>> {

        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id FROM job_run WHERE status IN "
        );
        push_finished_statuses(&mut query_builder);
        query_builder.push(
            " AND NOT EXISTS (SELECT 1 FROM job_run_notification n WHERE n.job_run_id = job_run.id AND n.status = "
        );
        query_builder.push_bind(JobRunNotificationStatus::Pending);
        query_builder.push(") ORDER BY id ASC");

        if limit != 0 {
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

    /// Five finished runs, keep_runs = 3: only the two oldest are candidates, whatever
    /// order the query happens to return them in.
    #[tokio::test]
    async fn select_deletable_job_runs_for_job_keeps_the_newest_n() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let mut candidates = db.crud.select_deletable_job_runs_for_job(&mut conn, "job", 3, 0).await.unwrap();
        candidates.sort();

        let mut expected = ids(&runs[0..2]);
        expected.sort();

        assert_eq!(candidates, expected);
    }

    /// The run occupying the oldest of the newest three still counts toward that window
    /// even though a `Pending` notification keeps it out of the result — so excluding it
    /// must not pull run three into the result to compensate. Filtering before ranking
    /// would do exactly that; this is the regression that rules it out.
    #[tokio::test]
    async fn a_pending_notification_excludes_a_run_without_shifting_the_others_rank() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        db.insert_job_run_notification(runs[0].id, NotifyOn::Failure, NotificationChannel::Email, &["oncall@example.com"]).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let candidates = db.crud.select_deletable_job_runs_for_job(&mut conn, "job", 3, 0).await.unwrap();

        assert_eq!(candidates, vec![runs[1].id]);
    }

    /// A `Sent` notification is done owing anything, so the run behind it is a candidate
    /// again — `select_deletable_job_runs_for_job` only ever excludes `Pending` rows.
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
        let mut candidates = db.crud.select_deletable_job_runs_for_job(&mut conn, "job", 3, 0).await.unwrap();
        candidates.sort();

        assert_eq!(candidates, vec![runs[0].id, runs[1].id]);
    }

    /// `Pending` and `Running` runs are never candidates, even when they are the oldest
    /// rows and `keep_runs` would otherwise put them past the kept window.
    #[tokio::test]
    async fn pending_and_running_runs_are_never_returned() {

        let db = TestDb::new().await;

        let pending = db.insert_job_run(JobRunStatus::Pending).await;
        let running = db.insert_job_run(JobRunStatus::Running).await;
        let finished = db.insert_job_run(JobRunStatus::Failed).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let candidates = db.crud.select_deletable_job_runs_for_job(&mut conn, "job", 0, 0).await.unwrap();

        assert!(!candidates.contains(&pending.id));
        assert!(!candidates.contains(&running.id));
        assert_eq!(candidates, vec![finished.id]);
    }

    /// `limit` caps how many candidates come back — 0 means uncapped, matching every other
    /// number in the retention plan.
    #[tokio::test]
    async fn select_deletable_job_runs_for_job_respects_its_limit() {

        let db = TestDb::new().await;

        for _ in 0..5 {
            db.insert_job_run(JobRunStatus::Succeeded).await;
        }

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let candidates = db.crud.select_deletable_job_runs_for_job(&mut conn, "job", 0, 2).await.unwrap();

        assert_eq!(candidates.len(), 2);
    }

    /// Oldest first, across every job — not partitioned by job the way the per-job select
    /// is — and truncated at `limit`.
    #[tokio::test]
    async fn select_oldest_deletable_job_runs_crosses_jobs_and_respects_its_limit() {

        let db = TestDb::new().await;

        let a1 = insert_finished_run_for(&db, "job-a", JobRunStatus::Succeeded).await;
        let b1 = insert_finished_run_for(&db, "job-b", JobRunStatus::Succeeded).await;
        let a2 = insert_finished_run_for(&db, "job-a", JobRunStatus::Succeeded).await;
        let _b2 = insert_finished_run_for(&db, "job-b", JobRunStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let oldest = db.crud.select_oldest_deletable_job_runs(&mut conn, 3).await.unwrap();

        assert_eq!(oldest, vec![a1.id, b1.id, a2.id]);
    }

    /// `Pending`, `Running` and notification-owing runs are excluded here exactly as they
    /// are from the per-job select.
    #[tokio::test]
    async fn select_oldest_deletable_job_runs_excludes_unfinished_and_pending_notification_runs() {

        let db = TestDb::new().await;

        let running = db.insert_job_run(JobRunStatus::Running).await;
        let owed = db.insert_job_run(JobRunStatus::Failed).await;
        db.insert_job_run_notification(owed.id, NotifyOn::Failure, NotificationChannel::Email, &["oncall@example.com"]).await;
        let deletable = db.insert_job_run(JobRunStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let oldest = db.crud.select_oldest_deletable_job_runs(&mut conn, 0).await.unwrap();

        assert_eq!(oldest, vec![deletable.id]);
        assert!(!oldest.contains(&running.id));
        assert!(!oldest.contains(&owed.id));
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
