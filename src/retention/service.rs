use std::sync::Arc;

use crate::app_config::AppConfig;
use crate::crud::CRUD;
use crate::declared::Declared;
use crate::crud::job_run::{DeleteJobRunsData, DeleteJobRunsDataFilter};
use crate::crud::multistatements::retention_candidates::{
    SelectDeletableJobRunsData, SelectDeletableJobRunsDataFilter,
};
use crate::poller::Service;
use crate::run_dir::remove_job_run_dir;


/// Deletes finished job runs old enough that nothing needs them anymore, so a long-lived
/// project directory cannot fill the disk.
///
/// Not part of the orchestrator — that module turns job runs into finished task runs, and
/// this does the opposite of turning anything into anything — and started the way the
/// notification service is: alongside the orchestrator, on its own `Poller`, rather than
/// inside it.
pub struct RetentionService {
    pub crud: Arc<CRUD>,
    /// Read for each job's `keep_runs`.
    pub declared: Arc<Declared>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub app_config: AppConfig,
}


/// The most runs one pass deletes, so the first pass against a large backlog cannot hold
/// the single SQLite writer for minutes. Later passes take the rest.
const MAX_DELETES_PER_PASS: u32 = 100;


impl RetentionService {

    pub fn new(
        crud: Arc<CRUD>,
        declared: Arc<Declared>,
        conn_pool: Arc<sqlx::SqlitePool>,
        app_config: AppConfig,
    ) -> Self {
        Self {
            crud,
            declared,
            conn_pool,
            app_config,
        }
    }

    /// Every finished job run past its job's own `keep_runs`, one job at a time.
    ///
    /// A job id `declared` does not hold falls back to `[job_defaults] keep_runs`, not to
    /// "no limit" — unlike `is_job_at_max_parallel_runs`, which treats a missing job as
    /// having no cap at all. That is harmless there, since such a job declares no cap to
    /// enforce; here such a job can still own years of finished runs (an ad-hoc job, or
    /// one whose YAML was deleted), and defaulting to "keep everything" would let exactly
    /// the jobs retention most needs to reach opt out by disappearing.
    ///
    /// **`keep_runs == 0` skips the job entirely** rather than calling
    /// `select_deletable_job_runs` with no offset: `offset: None` and `offset: Some(0)` both
    /// mean "no offset", so that call would return every finished run of the job as
    /// deletable — the exact opposite of `keep_runs = 0`'s meaning, "keep every run of this
    /// job".
    async fn select_per_job_candidates(
        &self,
        conn: &mut sqlx::SqliteConnection,
    ) -> anyhow::Result<Vec<i64>> {

        let mut ids = Vec::new();

        let job_ids = self.crud.select_job_ids_with_finished_job_runs(conn).await?;

        for job_id in job_ids {

            let keep_runs = match self.declared.job(&job_id) {
                Some(job) => job.keep_runs,
                None => self.app_config.job_defaults.keep_runs,
            };

            if keep_runs == 0 {
                continue;
            }

            let remaining = MAX_DELETES_PER_PASS.saturating_sub(ids.len() as u32);

            if remaining == 0 {
                break;
            }

            let candidates = self.crud.select_deletable_job_runs(conn, &SelectDeletableJobRunsData {
                filter: SelectDeletableJobRunsDataFilter { job_id: Some(job_id) },
                offset: Some(keep_runs),
                limit: Some(remaining),
            }).await?;

            ids.extend(candidates);
        }

        Ok(ids)
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

        let ids = self.select_per_job_candidates(&mut conn).await?;

        // The Poller has no per-pass hook, and a line per deleted run would be a hundred
        // lines for one pass, so the summary is printed here rather than once per `handle`.
        if !ids.is_empty() {
            println!("retention deleting {} finished job runs over their job's keep_runs", ids.len());
        }

        Ok(ids)
    }

    async fn handle(&self, row: &i64) -> anyhow::Result<()> {

        let mut conn = self.conn_pool.acquire().await?;

        // The run's descendants are deleted with it, and a parent that did not wait for its
        // children can finish before them - so it is left for a later pass until every one
        // has finished, rather than taking a running child with it.
        let descendants = self.crud.select_job_run_descendants(&mut conn, *row).await?;

        if descendants.iter().any(|descendant| !descendant.status.is_finished()) {
            return Ok(());
        }

        // Never an entirely empty filter: that would match every row in job_run and
        // delete every job run in the database, by design of
        // `delete_job_runs_with_children` — see that method's own doc comment.
        let deleted = self.crud.delete_job_runs_with_children(&mut conn, &DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter {
                id: Some(*row),
                job_id: None,
                status: None,
                schedule_id: None,
                scheduled_at_gt: None,
            },
        }).await?;

        // After the rows, and logged rather than raised. The run is already gone, so
        // returning an error here would have the next pass re-select an id that no longer
        // exists and fail on it for ever. A directory left behind is a directory; a poller
        // that cannot get past one row is every run of every job stopping.
        for job_run_id in deleted {
            if let Err(error) = remove_job_run_dir(&self.app_config.project_dir, job_run_id) {
                eprintln!(
                    "Job run {} was deleted but its directory could not be removed: {:#}",
                    job_run_id, error,
                );
            }
        }

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use chrono::Utc;
    use crate::app_config::{AppConfig, AppConfigJobDefaults};
    use crate::crud::job_run::{JobRun, JobRunStatus};
    use crate::crud::job_run_notification::{NotificationChannel, NotifyOn};
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt_output::{
        InsertTaskRunAttemptOutputData, InsertTaskRunAttemptOutputDataInput, TaskRunAttemptOutputStream,
    };
    use crate::poller::Service;
    use crate::retention::RetentionService;
    use super::MAX_DELETES_PER_PASS;
    use crate::test_support::TestDb;

    fn service_for(db: &TestDb) -> RetentionService {
        RetentionService::new(db.crud.clone(), std::sync::Arc::new(db.declared()), db.conn_pool.clone(), db.app_config())
    }

    fn service_with_config(db: &TestDb, app_config: AppConfig) -> RetentionService {
        RetentionService::new(db.crud.clone(), std::sync::Arc::new(db.declared()), db.conn_pool.clone(), app_config)
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
                    parent_task_run_attempt_id: None,
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

        let db = TestDb::new().await;
        db.insert_job("job", 3);

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

        let db = TestDb::new().await;
        db.insert_job("job", 1);

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

        let db = TestDb::new().await;
        db.insert_job("job", 1);

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

        let db = TestDb::new().await;

        let deleted = full_job_run(&db).await;
        let kept = full_job_run(&db).await;

        let service = service_for(&db);
        service.handle(&deleted.id).await.unwrap();

        let deleted_job_run = db.crud.select_job_run(
            &*db.conn_pool,
            &crate::crud::job_run::SelectJobRunsData {
                filter: crate::crud::job_run::SelectJobRunsDataFilter { id: Some(deleted.id), job_id: None, status: None, statuses: None, schedule_id: None, scheduled_at: None, parent_job_run_id: None },
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
                filter: crate::crud::task_run_attempt::SelectTaskRunAttemptsDataFilter { id: None, task_run_id: None, job_run_id: Some(deleted.id), task_id: None, status: None },
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
                filter: crate::crud::job_run::SelectJobRunsDataFilter { id: Some(kept.id), job_id: None, status: None, statuses: None, schedule_id: None, scheduled_at: None, parent_job_run_id: None },
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

    /// Retention is the only thing that deletes a run directory, which is the filesystem
    /// responsibility this service did not have before the run directory existed.
    #[tokio::test]
    async fn deleting_a_run_removes_its_directory() {
        let db = TestDb::new().await;

        let job_run = insert_finished_run_for(&db, "job", JobRunStatus::Succeeded).await;

        let dir = crate::run_dir::create_job_run_dir(&db.app_config().project_dir, job_run.id).unwrap();
        std::fs::write(dir.join("work.txt"), "something").unwrap();

        service_for(&db).handle(&job_run.id).await.unwrap();

        assert!(!dir.exists());
    }

    /// A parent that did not wait for its child can finish first. Deleting it then would
    /// delete the child with it, still running, so it waits for a pass after the child is
    /// done - and takes the child's directory with it.
    #[tokio::test]
    async fn a_parent_is_kept_until_the_runs_it_submitted_have_finished() {
        let db = TestDb::new().await;

        let parent = db.insert_job_run(JobRunStatus::Succeeded).await;
        let task_run = db.insert_task_run(parent.id, TaskRunStatus::Succeeded).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, crate::crud::task_run_attempt::TaskRunAttemptStatus::Succeeded).await;
        let child = db.insert_child_job_run(&attempt, JobRunStatus::Running).await;

        let child_dir = crate::run_dir::job_run_dir(&db.app_config().project_dir, child.id).unwrap();

        service_for(&db).handle(&parent.id).await.unwrap();

        assert_eq!(db.job_run(parent.id).await.id, parent.id);
        assert!(child_dir.exists());

        db.crud.update_job_runs(&*db.conn_pool, &crate::crud::job_run::UpdateJobRunsData {
            input: crate::crud::job_run::UpdateJobRunsDataInput {
                status: Some(JobRunStatus::Succeeded),
                ..Default::default()
            },
            filter: crate::crud::job_run::UpdateJobRunsDataFilter { id: Some(child.id) },
        }).await.unwrap();

        service_for(&db).handle(&parent.id).await.unwrap();

        let parent_left = db.crud.select_job_run(&*db.conn_pool, &crate::crud::job_run::SelectJobRunsData {
            filter: crate::crud::job_run::SelectJobRunsDataFilter { id: Some(parent.id), ..Default::default() },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await.unwrap();
        assert!(parent_left.is_none());
        assert!(!child_dir.exists());
    }

    #[tokio::test]
    async fn deleting_a_run_leaves_another_runs_directory_alone() {
        let db = TestDb::new().await;

        let job_run = insert_finished_run_for(&db, "job", JobRunStatus::Succeeded).await;
        let neighbour = insert_finished_run_for(&db, "job", JobRunStatus::Succeeded).await;

        crate::run_dir::create_job_run_dir(&db.app_config().project_dir, job_run.id).unwrap();

        let neighbour_dir = crate::run_dir::create_job_run_dir(&db.app_config().project_dir, neighbour.id).unwrap();

        service_for(&db).handle(&job_run.id).await.unwrap();

        assert!(neighbour_dir.is_dir());
    }

    /// The rows are gone by the time the directory is touched, so a directory that cannot
    /// be removed must not fail the pass: the next one would re-select an id that no
    /// longer exists and never get past it.
    #[tokio::test]
    async fn a_directory_that_is_already_gone_does_not_fail_the_pass() {
        let db = TestDb::new().await;

        let job_run = insert_finished_run_for(&db, "job", JobRunStatus::Succeeded).await;

        // Never created, which is also the state of a run that was skipped before it was
        // ever released.

        service_for(&db).handle(&job_run.id).await.unwrap();
    }

    /// `keep_runs = 0` keeps every run of that job. Read as "no offset" instead, it would
    /// hand back every one of them, the newest included.
    #[tokio::test]
    async fn keep_runs_zero_keeps_every_run() {

        let db = TestDb::new().await;
        db.insert_job("job", 0);

        for _ in 0..3 {
            db.insert_job_run(JobRunStatus::Succeeded).await;
        }

        assert!(service_for(&db).select().await.unwrap().is_empty());
    }

    /// Test 7: a run whose job nothing declares is judged by [job_defaults] keep_runs.
    #[tokio::test]
    async fn a_job_nothing_declares_is_judged_by_job_defaults_keep_runs() {

        let db = TestDb::new().await;

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

    /// A pass selects at most `MAX_DELETES_PER_PASS`, the oldest first, and the next pass
    /// continues from there.
    #[tokio::test]
    async fn a_pass_is_capped_at_max_deletes_per_pass_and_the_next_pass_continues() {

        let db = TestDb::new().await;
        db.insert_job("job", 1);

        let total = MAX_DELETES_PER_PASS as usize + 3;

        let mut runs = Vec::new();
        for _ in 0..total {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let service = service_for(&db);

        let mut first_pass = service.select().await.unwrap();
        first_pass.sort();

        let oldest: Vec<i64> = runs[..MAX_DELETES_PER_PASS as usize].iter().map(|run| run.id).collect();
        assert_eq!(first_pass, oldest);

        for id in &first_pass {
            service.handle(id).await.unwrap();
        }

        let mut second_pass = service.select().await.unwrap();
        second_pass.sort();
        assert_eq!(second_pass, vec![runs[total - 3].id, runs[total - 2].id]);
    }

    /// Test 9: the newest finished run of a job is never selected by the per-job rule.
    #[tokio::test]
    async fn the_newest_run_of_a_job_is_never_selected() {

        let db = TestDb::new().await;
        db.insert_job("job", 1);

        let mut runs = Vec::new();
        for _ in 0..4 {
            runs.push(db.insert_job_run(JobRunStatus::Succeeded).await);
        }

        let service = service_for(&db);
        let ids = service.select().await.unwrap();

        assert!(!ids.is_empty());
        assert!(!ids.contains(&runs[3].id));
    }
}
