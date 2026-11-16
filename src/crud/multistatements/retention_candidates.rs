//! Which finished job runs may be deleted — one operation spanning `job_run` and
//! `job_run_notification`, so it belongs to neither entity file. Nothing here writes
//! anything; a run's rows disappear through seven other places — the single-table
//! `delete_job_runs` in `job_run.rs` and its five sibling entity deletes, plus
//! `delete_job_runs_with_children`, which calls all six as one cascade.
//!
//! Neither method decides *how many* to delete, or resolves a job's own
//! `keep_runs` — that policy belongs to the retention service. They only answer, for a
//! given shape of question, which finished runs are candidates right now.
//!
//! No statement is written here: each method composes `job_run`'s and
//! `job_run_notification`'s own basic CRUD methods, the way every multistatement does.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::{
    CountJobRunsData, JobRunStatus, SelectJobRunJobIdsData, SelectJobRunsData,
    SelectJobRunsDataFilter, SelectJobRunsDataSort,
};
use crate::crud::job_run_notification::{
    JobRunNotificationStatus, SelectJobRunNotificationsData, SelectJobRunNotificationsDataFilter,
};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SelectDeletableJobRunsDataFilter {
    pub job_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectDeletableJobRunsData {
    pub filter: SelectDeletableJobRunsDataFilter,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl CRUD {

    /// Every job id that owns at least one finished run, including one no YAML declares:
    /// an ad-hoc definition, or a job whose YAML has since been deleted. It is what the
    /// per-job pass iterates over, since nothing else enumerates "jobs that have ever run"
    /// the way `Declared::jobs` enumerates "jobs currently declared".
    pub async fn select_job_ids_with_finished_job_runs(&self, conn: &mut SqliteConnection) -> anyhow::Result<Vec<String>> {

        let job_ids = self.select_job_run_job_ids(&mut *conn, &SelectJobRunJobIdsData {
            filter: finished_job_runs_filter(None),
        }).await?;

        Ok(job_ids)
    }

    /// Finished runs matching `data.filter` past the newest `data.offset` of them, minus any
    /// that still owe a `Pending` notification, capped at `data.limit`.
    ///
    /// **The result is ordered oldest-first**, because deletion proceeds oldest-first: a `limit` smaller than the candidate set has to bite
    /// the newest end of it, so that a backlog is worked down from the old end and a
    /// half-finished backlog leaves a trimmed tail rather than a hole in the middle of the
    /// history.
    ///
    /// `offset` **is** `keep_runs`, and **`offset: None` and `offset: Some(0)` both mean no
    /// offset**, so every finished run of the matched jobs is a candidate; deleting all of
    /// them is irreversible.
    ///
    /// `limit: None` means no cap at all, since SQLite's `LIMIT 0` means zero rows rather
    /// than unlimited.
    ///
    /// **The ranking happens before the notification exclusion**, on purpose: the window is
    /// taken from `job_run` alone, and only the runs it hands back are then checked against
    /// the `Pending` notifications. A run awaiting delivery still occupies its ranked slot,
    /// so excluding it must never pull a neighbour across the `offset` line to compensate.
    ///
    /// **`limit` bounds the window, not the result**, which is the one thing that differs
    /// from doing all of this in a single statement: a run excluded for owing a notification
    /// is dropped from an already-limited window rather than replaced, so a pass can return
    /// fewer than `limit` ids. That under-delete is the safe direction — the next pass
    /// recomputes and takes what is left — and it keeps `limit` a real bound on how many
    /// rows the query reads.
    pub async fn select_deletable_job_runs(&self, conn: &mut SqliteConnection, data: &SelectDeletableJobRunsData) -> anyhow::Result<Vec<i64>> {

        let filter = finished_job_runs_filter(data.filter.job_id.clone());

        // The window is read oldest-first, so the offset that protects the newest runs is
        // turned into a cap instead: the oldest `matched - offset` runs are the candidates.
        // Skipping the newest `offset` of a descending read and taking `limit` of the rest
        // would take the *newest* candidates. The count is taken before any exclusion, so a
        // run owing a notification still fills its slot.
        let matched = self.count_job_runs(&mut *conn, &CountJobRunsData {
            filter: filter.clone(),
        }).await?;

        // Clamped at zero by hand: `i64::saturating_sub` saturates at `i64::MIN`, and a job
        // with fewer runs than its `keep_runs` would then ask for a negative limit — which
        // SQLite reads as no limit at all, handing back every run of a job that should have
        // been left entirely alone.
        let unprotected = (matched - data.offset.unwrap_or(0) as i64).max(0);

        let limit = match data.limit {
            None => unprotected,
            Some(limit) => (limit as i64).min(unprotected),
        };

        let window = self.select_job_runs(&mut *conn, &SelectJobRunsData {
            filter,
            sort: Some(SelectJobRunsDataSort::Id),
            limit: Some(limit),
            offset: None,
        }).await?;

        let pending = self.select_job_run_notifications(&mut *conn, &SelectJobRunNotificationsData {
            filter: SelectJobRunNotificationsDataFilter {
                status: Some(JobRunNotificationStatus::Pending),
                ..Default::default()
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

        // Every open notification in the database, rather than one lookup per candidate:
        // Pending is a transient state a row leaves as soon as its run settles, so this set
        // is a handful of rows however long the history is.
        let owing: HashSet<i64> = pending.into_iter()
            .map(|notification| notification.job_run_id)
            .collect();

        Ok(window.into_iter()
            .map(|job_run| job_run.id)
            .filter(|id| !owing.contains(id))
            .collect())
    }
}

/// Matches every finished run of `job_id`, or of every job when it is `None`. The statuses
/// come from `JobRunStatus::ALL` and the exhaustive `is_finished` match rather than a list
/// written out here, so a status added later has to say which side of it falls on before
/// any of this compiles at all.
fn finished_job_runs_filter(job_id: Option<String>) -> SelectJobRunsDataFilter {

    let finished = JobRunStatus::ALL.iter()
        .filter(|status| status.is_finished())
        .copied()
        .collect();

    SelectJobRunsDataFilter {
        job_id,
        statuses: Some(finished),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use chrono::Utc;

    use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRun, JobRunStatus};
    use crate::crud::job_run_notification::{
        JobRunNotificationStatus, NotificationChannel, NotifyOn, UpdateJobRunNotificationsData,
        UpdateJobRunNotificationsDataFilter, UpdateJobRunNotificationsDataInput,
    };
    use crate::crud::multistatements::retention_candidates::{
        SelectDeletableJobRunsData, SelectDeletableJobRunsDataFilter,
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
                    scheduled_at: Utc::now(),
                    schedule_id: None,
                    status,
                    parent_task_run_attempt_id: None,
                },
            },
        ).await.unwrap();

        db.job_run(id).await
    }

    fn ids(runs: &[JobRun]) -> Vec<i64> {
        runs.iter().map(|run| run.id).collect()
    }

    /// Per-job window: `offset: Some(keep_runs)` — five finished
    /// runs, `keep_runs = 3`: only the two oldest are candidates.
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

        db.crud.update_job_run_notifications(&*db.conn_pool, &UpdateJobRunNotificationsData {
            input: UpdateJobRunNotificationsDataInput {
                status: Some(JobRunNotificationStatus::Sent),
                ..Default::default()
            },
            filter: UpdateJobRunNotificationsDataFilter { id: Some(notification.id) },
        }).await.unwrap();

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let mut candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            limit: None,
            offset: Some(3),
        }).await.unwrap();
        candidates.sort();

        assert_eq!(candidates, vec![runs[0].id, runs[1].id]);
    }

    /// `Queued` and `Running` runs are never candidates, even when they are the oldest
    /// rows and an offset of `0` would otherwise put them past the kept window.
    #[tokio::test]
    async fn queued_and_running_runs_are_never_returned() {

        let db = TestDb::new().await;

        let queued = db.insert_job_run(JobRunStatus::Queued).await;
        let running = db.insert_job_run(JobRunStatus::Running).await;
        let finished = db.insert_job_run(JobRunStatus::Failed).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            limit: None,
            offset: None,
        }).await.unwrap();

        assert!(!candidates.contains(&queued.id));
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
            limit: Some(2),
            offset: None,
        }).await.unwrap();

        assert_eq!(candidates.len(), 2);
    }

    /// The per-job window under a budget takes the **oldest** deletable runs, not the
    /// newest of them: five finished runs with `keep_runs = 3` leave two candidates, and a
    /// `limit` of one must be the older of those two.
    ///
    /// Taking the newer instead converges on the same end state, but works backwards
    /// through the history one pass at a time — an operator who stops the server partway
    /// through a large backlog is then left with a hole in the middle of their run history
    /// rather than a trimmed tail.
    #[tokio::test]
    async fn a_limited_per_job_window_takes_the_oldest_deletable_run() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let candidates = db.crud.select_deletable_job_runs(&mut conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            limit: Some(1),
            offset: Some(3),
        }).await.unwrap();

        assert_eq!(candidates, vec![runs[0].id]);
    }

    /// The documented under-delete: `limit` bounds the ranked window, and a run owing a
    /// `Pending` notification *inside* that window is dropped from it rather than replaced,
    /// so the pass hands back fewer ids than `limit`.
    ///
    /// Six finished runs with `keep_runs = 3` leave three candidates, and a `limit` of two
    /// takes the oldest two of them — one of which owes a notification, so only one id comes
    /// back. Nothing is lost by it: once the notification is no longer `Pending` the next
    /// pass takes that run too. Fetching the window under its limit and then excluding is
    /// the only ordering that keeps the limit a bound on the *query*; deleting less than the
    /// budget allows is the safe direction this whole design takes.
    #[tokio::test]
    async fn an_excluded_run_inside_the_limited_window_under_delivers_the_limit() {

        let db = TestDb::new().await;

        let mut runs = Vec::new();
        for _ in 0..6 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let notification = db.insert_job_run_notification(runs[0].id, NotifyOn::Failure, NotificationChannel::Email, &["oncall@example.com"]).await;

        let data = SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: Some("job".to_string()) },
            limit: Some(2),
            offset: Some(3),
        };

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let under_delivered = db.crud.select_deletable_job_runs(&mut conn, &data).await.unwrap();

        assert_eq!(under_delivered, vec![runs[1].id]);

        db.crud.update_job_run_notifications(&*db.conn_pool, &UpdateJobRunNotificationsData {
            input: UpdateJobRunNotificationsDataInput {
                status: Some(JobRunNotificationStatus::Sent),
                ..Default::default()
            },
            filter: UpdateJobRunNotificationsDataFilter { id: Some(notification.id) },
        }).await.unwrap();

        let next_pass = db.crud.select_deletable_job_runs(&mut conn, &data).await.unwrap();

        assert_eq!(next_pass, vec![runs[0].id, runs[1].id]);
    }

    /// Every job id owning a finished run comes back, including one the `TestDb` declares
    /// no job for.
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
