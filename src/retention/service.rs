use std::collections::HashSet;
use std::sync::Arc;

use crate::app_config::AppConfig;
use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{DeleteJobRunsData, DeleteJobRunsDataFilter};
use crate::crud::multistatements::retention_candidates::{
    SelectDeletableJobRunsData, SelectDeletableJobRunsDataFilter, SelectDeletableJobRunsDataSort,
};
use crate::poller::Service;


/// Deletes finished job runs old enough that nothing needs them anymore, so a long-lived
/// data directory cannot fill the disk.
///
/// Not part of the orchestrator — that module turns job runs into finished task runs, and
/// this does the opposite of turning anything into anything — and started the way the
/// notification service is: alongside the orchestrator, on its own `Poller`, rather than
/// inside it.
pub struct RetentionService {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub app_config: AppConfig,
}


/// `budget` of `0` means unbounded, the same convention `[retention]`'s own fields use —
/// `None` here plays exactly that role for `select_deletable_job_runs`'s `limit`, since
/// `LIMIT 0` there means zero rows rather than no limit at all.
fn remaining_budget(budget: u32, selected_so_far: u32) -> Option<u32> {
    if budget == 0 {
        None
    } else {
        Some(budget.saturating_sub(selected_so_far))
    }
}


impl RetentionService {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        app_config: AppConfig,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            app_config,
        }
    }

    /// Every finished job run past its job's own `keep_runs`, one job at a time.
    ///
    /// A job id with no row in `mem.job` falls back to `[job_defaults] keep_runs`, not to
    /// "no limit" — unlike `is_job_at_max_parallel_runs`, which treats a missing job as
    /// having no cap at all. That is safe there because a job with no row can never have a
    /// running job run to count; here a job with no row can still own years of finished
    /// runs (an ad-hoc job, or one whose YAML was deleted), and defaulting to "keep
    /// everything" would let exactly the jobs retention most needs to reach opt out by
    /// disappearing.
    ///
    /// **`keep_runs == 0` skips the job entirely** rather than calling
    /// `select_deletable_job_runs` with no offset: `offset: None` and `offset: Some(0)` both
    /// mean "no offset", so that call would return every finished run of the job as
    /// deletable — the exact opposite of `keep_runs = 0`'s meaning, "keep every run of this
    /// job".
    async fn select_per_job_candidates(
        &self,
        conn: &mut sqlx::SqliteConnection,
        budget: u32,
    ) -> anyhow::Result<Vec<i64>> {

        let mut ids = Vec::new();

        let job_ids = self.crud.select_job_ids_with_finished_job_runs(conn).await?;

        for job_id in job_ids {

            let job = self.crud.select_job(&mut *conn, &SelectJobsData {
                filter: SelectJobsDataFilter { job_id: Some(job_id.clone()), name_like: None },
                sort: None,
                limit: None,
                offset: None,
            }).await?;

            let keep_runs = match job {
                Some(job) => job.keep_runs,
                None => self.app_config.job_defaults.keep_runs,
            };

            if keep_runs == 0 {
                continue;
            }

            let remaining = remaining_budget(budget, ids.len() as u32);

            let candidates = self.crud.select_deletable_job_runs(conn, &SelectDeletableJobRunsData {
                filter: SelectDeletableJobRunsDataFilter { job_id: Some(job_id) },
                sort: Some(SelectDeletableJobRunsDataSort::NewestFirst),
                offset: Some(keep_runs),
                limit: remaining,
            }).await?;

            ids.extend(candidates);
        }

        Ok(ids)
    }

    /// Every finished job run past the global `[retention] keep_runs_total` ceiling, oldest
    /// first across every job, minus whatever the per-job pass already collected.
    ///
    /// `already_selected` is only ever the per-job ids from this same pass — the overflow
    /// this computes is not reduced by them, on purpose: modelling "what the per-job
    /// deletions would already bring the count down to" would make this depend on the
    /// per-job pass succeeding exactly as planned, and a run selected by both rules is
    /// simply deleted once, on whichever pass gets to it. Under-counting here is always the
    /// safe direction, since a later pass recomputes from what is actually still there.
    async fn select_global_overflow_candidates(
        &self,
        conn: &mut sqlx::SqliteConnection,
        budget: u32,
        already_selected: &[i64],
    ) -> anyhow::Result<Vec<i64>> {

        let keep_runs_total = self.app_config.retention.keep_runs_total;

        if keep_runs_total == 0 {
            return Ok(Vec::new());
        }

        let finished = self.crud.count_finished_job_runs(conn).await?;
        let overflow = finished.saturating_sub(keep_runs_total);

        if overflow == 0 {
            return Ok(Vec::new());
        }

        let remaining = remaining_budget(budget, already_selected.len() as u32);

        let limit = match remaining {
            None => Some(overflow),
            Some(remaining) => Some(overflow.min(remaining)),
        };

        let candidates = self.crud.select_deletable_job_runs(conn, &SelectDeletableJobRunsData {
            filter: SelectDeletableJobRunsDataFilter { job_id: None },
            sort: Some(SelectDeletableJobRunsDataSort::OldestFirst),
            offset: None,
            limit,
        }).await?;

        let already: HashSet<i64> = already_selected.iter().copied().collect();

        Ok(candidates.into_iter().filter(|id| !already.contains(id)).collect())
    }
}


impl Service for RetentionService {
    type Row = i64;

    fn name(&self) -> &'static str {
        "retention"
    }

    fn row_context(&self, row: &i64) -> String {
        format!("job run {row}")
    }

    async fn select(&self) -> anyhow::Result<Vec<i64>> {

        let mut conn = self.conn_pool.acquire().await?;

        let budget = self.app_config.retention.max_deletes_per_pass;

        let per_job_ids = self.select_per_job_candidates(&mut conn, budget).await?;

        let global_ids = self.select_global_overflow_candidates(
            &mut conn,
            budget,
            &per_job_ids,
        ).await?;

        // The Poller has no per-pass hook, and a line per deleted run would be a hundred
        // lines for one pass under `max_deletes_per_pass`, so the summary is printed here,
        // the one place that already knows the split, rather than once per `handle`.
        if !per_job_ids.is_empty() || !global_ids.is_empty() {
            println!(
                "retention deleting {} finished job runs ({} over per-job keep_runs, {} over keep_runs_total)",
                per_job_ids.len() + global_ids.len(),
                per_job_ids.len(),
                global_ids.len(),
            );
        }

        Ok(per_job_ids.into_iter().chain(global_ids).collect())
    }

    async fn handle(&self, row: &i64) -> anyhow::Result<()> {

        let mut conn = self.conn_pool.acquire().await?;

        // Never an entirely empty filter: that would match every row in job_run and
        // delete every job run in the database, by design of
        // `delete_job_runs_with_children` — see that method's own doc comment.
        self.crud.delete_job_runs_with_children(&mut conn, &DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter {
                id: Some(*row),
                job_id: None,
                status: None,
                schedule_id: None,
                scheduled_at_gt: None,
            },
        }).await
    }
}


#[cfg(test)]
mod tests {
    use chrono::Utc;
    use crate::app_config::{AppConfig, AppConfigJobDefaults, AppConfigRetention};
    use crate::crud::job_run::{JobRun, JobRunStatus};
    use crate::crud::job_run_notification::{NotificationChannel, NotifyOn};
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt_output::{
        InsertTaskRunAttemptOutputData, InsertTaskRunAttemptOutputDataInput, TaskRunAttemptOutputStream,
    };
    use crate::poller::Service;
    use crate::retention::RetentionService;
    use crate::test_support::TestDb;

    fn service_for(db: &TestDb) -> RetentionService {
        RetentionService::new(db.crud.clone(), db.conn_pool.clone(), db.app_config())
    }

    fn service_with_config(db: &TestDb, app_config: AppConfig) -> RetentionService {
        RetentionService::new(db.crud.clone(), db.conn_pool.clone(), app_config)
    }

    /// A finished run under a caller-chosen job id, for the tests that need more than one
    /// job — `TestDb::insert_job_run` always writes `job_id: "job"`. Mirrors the helper of
    /// the same name in `retention_candidates.rs`'s own tests.
    async fn insert_finished_run_for(db: &TestDb, job_id: &str, status: JobRunStatus) -> JobRun {
        use std::collections::BTreeMap;
        use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput};

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
                },
            },
        ).await.unwrap();

        db.job_run(id).await
    }

    /// One job run carrying one row in every one of the six tables `handle` must clear,
    /// built the way a real run accumulates them. Mirrors `full_job_run` in
    /// `delete_job_runs_with_children.rs`'s own tests.
    async fn full_job_run(db: &TestDb) -> JobRun {

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Failed).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, crate::crud::task_run_attempt::TaskRunAttemptStatus::Failed).await;

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

        job_run
    }

    /// Test 1: a job with keep_runs = 3 and five finished runs keeps the newest three.
    #[tokio::test]
    async fn keep_runs_keeps_the_newest_n() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;
        db.insert_job("job", 3).await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let service = service_for(&db);
        let mut ids = service.select().await.unwrap();
        ids.sort();

        let mut expected = vec![runs[0].id, runs[1].id];
        expected.sort();

        assert_eq!(ids, expected);
    }

    /// Test 2: Queued and Running runs are never selected, including when they are the
    /// oldest rows and the ceiling is exceeded.
    #[tokio::test]
    async fn queued_and_running_runs_are_never_selected() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;
        db.insert_job("job", 1).await;

        let queued = db.insert_job_run(JobRunStatus::Queued).await;
        let running = db.insert_job_run(JobRunStatus::Running).await;
        let f1 = db.insert_job_run(JobRunStatus::Succeeded).await;
        let f2 = db.insert_job_run(JobRunStatus::Succeeded).await;
        let f3 = db.insert_job_run(JobRunStatus::Succeeded).await;

        let service = service_for(&db);
        let mut ids = service.select().await.unwrap();
        ids.sort();

        let mut expected = vec![f1.id, f2.id];
        expected.sort();

        assert_eq!(ids, expected);
        assert!(!ids.contains(&queued.id));
        assert!(!ids.contains(&running.id));
        assert!(!ids.contains(&f3.id));
    }

    /// Test 3: a finished run with a Pending notification survives; once the row is Sent
    /// it is selected on a later pass.
    #[tokio::test]
    async fn a_pending_notification_survives_until_sent() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;
        db.insert_job("job", 1).await;

        let old = db.insert_job_run(JobRunStatus::Succeeded).await;
        let _new = db.insert_job_run(JobRunStatus::Succeeded).await;

        let notification = db.insert_job_run_notification(
            old.id,
            NotifyOn::Failure,
            NotificationChannel::Email,
            &["oncall@example.com"],
        ).await;

        let service = service_for(&db);

        assert!(service.select().await.unwrap().is_empty());

        sqlx::query("UPDATE job_run_notification SET status = 'sent' WHERE id = ?")
            .bind(notification.id)
            .execute(&*db.conn_pool)
            .await
            .unwrap();

        assert_eq!(service.select().await.unwrap(), vec![old.id]);
    }

    /// Test 4: handle on a selected run removes its rows from all six tables and leaves
    /// another run's rows untouched.
    #[tokio::test]
    async fn handle_removes_all_six_tables_and_leaves_a_neighbour_intact() {

        // `handle` deletes by job run id and never resolves a job's `keep_runs`, so nothing
        // here reads `mem.job` - a plain `TestDb` is enough.
        let db = TestDb::new().await;

        let deleted = full_job_run(&db).await;
        let kept = full_job_run(&db).await;

        let service = service_for(&db);
        service.handle(&deleted.id).await.unwrap();

        let deleted_job_run = db.crud.select_job_run(
            &*db.conn_pool,
            &crate::crud::job_run::SelectJobRunsData {
                filter: crate::crud::job_run::SelectJobRunsDataFilter { id: Some(deleted.id), job_id: None, status: None, statuses: None, schedule_id: None, scheduled_at: None },
                sort: None,
                limit: Some(1),
                offset: None,
            },
        ).await.unwrap();
        assert!(deleted_job_run.is_none());

        let deleted_task_runs = db.crud.select_task_runs(
            &*db.conn_pool,
            &crate::crud::task_run::SelectTaskRunsData {
                filter: crate::crud::task_run::SelectTaskRunsDataFilter { id: None, job_run_id: Some(deleted.id), job_id: None, task_id: None, status: None },
                sort: None,
            },
        ).await.unwrap();
        assert!(deleted_task_runs.is_empty());

        let deleted_attempts = db.crud.select_task_run_attempts(
            &*db.conn_pool,
            &crate::crud::task_run_attempt::SelectTaskRunAttemptsData {
                filter: crate::crud::task_run_attempt::SelectTaskRunAttemptsDataFilter { task_run_id: None, job_run_id: Some(deleted.id), task_id: None, status: None },
                sort: None,
            },
        ).await.unwrap();
        assert!(deleted_attempts.is_empty());

        let deleted_outputs = db.crud.select_task_run_attempt_outputs(
            &*db.conn_pool,
            &crate::crud::task_run_attempt_output::SelectTaskRunAttemptOutputsData {
                filter: crate::crud::task_run_attempt_output::SelectTaskRunAttemptOutputsDataFilter { id: None, task_run_attempt_id: None, task_run_id: None, job_run_id: Some(deleted.id), job_id: None, task_id: None, stream: None },
                sort: None,
            },
        ).await.unwrap();
        assert!(deleted_outputs.is_empty());

        let deleted_stops = db.crud.select_job_run_stops(
            &*db.conn_pool,
            &crate::crud::job_run_stop::SelectJobRunStopsData {
                filter: crate::crud::job_run_stop::SelectJobRunStopsDataFilter { id: None, job_run_id: Some(deleted.id) },
                sort: None,
                limit: None,
                offset: None,
            },
        ).await.unwrap();
        assert!(deleted_stops.is_empty());

        assert!(db.job_run_notifications(deleted.id).await.is_empty());

        // The neighbour, untouched.
        let kept_job_run = db.crud.select_job_run(
            &*db.conn_pool,
            &crate::crud::job_run::SelectJobRunsData {
                filter: crate::crud::job_run::SelectJobRunsDataFilter { id: Some(kept.id), job_id: None, status: None, statuses: None, schedule_id: None, scheduled_at: None },
                sort: None,
                limit: Some(1),
                offset: None,
            },
        ).await.unwrap();
        assert!(kept_job_run.is_some());

        let kept_task_runs = db.crud.select_task_runs(
            &*db.conn_pool,
            &crate::crud::task_run::SelectTaskRunsData {
                filter: crate::crud::task_run::SelectTaskRunsDataFilter { id: None, job_run_id: Some(kept.id), job_id: None, task_id: None, status: None },
                sort: None,
            },
        ).await.unwrap();
        assert_eq!(kept_task_runs.len(), 1);
        assert_eq!(db.job_run_notifications(kept.id).await.len(), 1);
    }

    /// Test 5: keep_runs = 0 keeps every run of that job — the per-job rule selects
    /// nothing for it — while keep_runs_total still applies to it.
    #[tokio::test]
    async fn keep_runs_zero_is_skipped_by_the_per_job_rule_but_not_by_the_global_ceiling() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;
        db.insert_job("job", 0).await;

        let run1 = db.insert_job_run(JobRunStatus::Succeeded).await;
        let run2 = db.insert_job_run(JobRunStatus::Succeeded).await;
        let run3 = db.insert_job_run(JobRunStatus::Succeeded).await;

        let service = service_with_config(&db, AppConfig {
            retention: AppConfigRetention { keep_runs_total: 1, max_deletes_per_pass: 0 },
            ..db.app_config()
        });

        let mut ids = service.select().await.unwrap();
        ids.sort();

        // If keep_runs = 0 were mistakenly treated as "no offset", the per-job rule would
        // hand back every one of this job's three runs, including the newest — the global
        // ceiling would then have nothing left to add and could not save it either.
        let mut expected = vec![run1.id, run2.id];
        expected.sort();

        assert_eq!(ids, expected);
        assert!(!ids.contains(&run3.id));
    }

    /// Test 6: keep_runs_total selects oldest-first across jobs, and only the overflow.
    #[tokio::test]
    async fn keep_runs_total_selects_oldest_first_across_jobs() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;

        let a1 = insert_finished_run_for(&db, "job-a", JobRunStatus::Succeeded).await;
        let b1 = insert_finished_run_for(&db, "job-b", JobRunStatus::Succeeded).await;
        let a2 = insert_finished_run_for(&db, "job-a", JobRunStatus::Succeeded).await;
        let _b2 = insert_finished_run_for(&db, "job-b", JobRunStatus::Succeeded).await;

        // job_defaults.keep_runs stays at its ample default, so the per-job rule never
        // fires here — only the global ceiling does.
        let service = service_with_config(&db, AppConfig {
            retention: AppConfigRetention { keep_runs_total: 2, max_deletes_per_pass: 0 },
            ..db.app_config()
        });

        let mut ids = service.select().await.unwrap();
        ids.sort();

        let mut expected = vec![a1.id, b1.id];
        expected.sort();

        assert_eq!(ids, expected);
        assert!(!ids.contains(&a2.id));
    }

    /// Test 7: a run whose job_id has no row in mem is judged by [job_defaults] keep_runs.
    #[tokio::test]
    async fn a_job_with_no_mem_row_is_judged_by_job_defaults_keep_runs() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let app_config = db.app_config();

        let service = service_with_config(&db, AppConfig {
            job_defaults: AppConfigJobDefaults { keep_runs: 2, ..app_config.job_defaults.clone() },
            ..app_config
        });

        let mut ids = service.select().await.unwrap();
        ids.sort();

        let mut expected = vec![runs[0].id, runs[1].id, runs[2].id];
        expected.sort();

        assert_eq!(ids, expected);
    }

    /// Test 8: a pass selects at most max_deletes_per_pass; the next pass continues.
    #[tokio::test]
    async fn a_pass_is_capped_at_max_deletes_per_pass_and_the_next_pass_continues() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;
        db.insert_job("job", 0).await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let service = service_with_config(&db, AppConfig {
            retention: AppConfigRetention { keep_runs_total: 1, max_deletes_per_pass: 2 },
            ..db.app_config()
        });

        let mut first_pass = service.select().await.unwrap();
        first_pass.sort();
        assert_eq!(first_pass, vec![runs[0].id, runs[1].id]);

        for id in &first_pass {
            service.handle(id).await.unwrap();
        }

        let mut second_pass = service.select().await.unwrap();
        second_pass.sort();
        assert_eq!(second_pass, vec![runs[2].id, runs[3].id]);
    }

    /// Test 9: the newest finished run of a job is never selected by the per-job rule.
    #[tokio::test]
    async fn the_newest_run_of_a_job_is_never_selected() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;
        db.insert_job("job", 1).await;

        let mut runs = Vec::new();
        for _ in 0..4 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let service = service_for(&db);
        let ids = service.select().await.unwrap();

        assert!(!ids.is_empty());
        assert!(!ids.contains(&runs[3].id));
    }

    /// Test 10: a per-job pass too small to take every candidate takes the oldest of them.
    ///
    /// Five finished runs, `keep_runs = 3`, a budget of one: the two oldest are deletable
    /// and the older of those two is the one that goes. Taking the newer converges on the
    /// same end state, but only after working backwards through the history one pass at a
    /// time — so a server stopped partway through a backlog would leave a hole in the
    /// middle of the run history instead of a trimmed tail.
    #[tokio::test]
    async fn a_budgeted_per_job_pass_deletes_the_oldest_runs_first() {

        let (db, _mem_conn) = TestDb::new_with_migrated_mem().await;
        db.insert_job("job", 3).await;

        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        // No global ceiling, so the one delete this pass may make is the per-job rule's.
        let service = service_with_config(&db, AppConfig {
            retention: AppConfigRetention { keep_runs_total: 0, max_deletes_per_pass: 1 },
            ..db.app_config()
        });

        assert_eq!(service.select().await.unwrap(), vec![runs[0].id]);

        service.handle(&runs[0].id).await.unwrap();

        assert_eq!(service.select().await.unwrap(), vec![runs[1].id]);
    }
}
