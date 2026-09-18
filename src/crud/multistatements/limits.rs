//! What the orchestrator's two concurrency gates count against: runs in flight for one
//! job, and attempts running across every job. Only a Running row that is not waiting on
//! another run holds a slot.

use std::collections::BTreeMap;
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter};
use crate::crud::task_run_attempt::{
    SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttempt, TaskRunAttemptStatus,
};

impl CRUD {

    /// Every attempt whose row says Running: the one set the three attempt tallies below
    /// all start from, before each diverges on its own one-line predicate. They must agree
    /// on it - a limit counted against a different set than the cap is a gate that lets
    /// through what it says it is holding - and that is easier to keep true as one name
    /// than as the same filter literal written out three times.
    async fn select_running_attempts(
        &self,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Vec<TaskRunAttempt>> {

        self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: Some(TaskRunAttemptStatus::Running),
            },
            sort: None,
        }).await
    }

    /// Whether the job already has as many runs in flight as it allows. Only a running
    /// job run holds a slot — a queued one is waiting for exactly this answer — and a
    /// max_parallel_runs of 0 means the job has no limit at all.
    pub async fn is_job_at_max_parallel_runs(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
    ) -> anyhow::Result<bool> {

        let job = self.select_job(&mut *conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: Some(job_id.to_string()),
                name_like: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        let Some(job) = job else {
            return Ok(false);
        };

        if job.max_parallel_runs == 0 {
            return Ok(false);
        }

        let running_job_runs = self.select_job_runs(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: Some(job_id.to_string()),
                status: Some(JobRunStatus::Running),
                statuses: None,
                schedule_id: None,
                scheduled_at: None,
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

        Ok(running_job_runs.len() >= job.max_parallel_runs as usize)
    }

    /// How many task run attempts are Running right now, across every job - what
    /// `TaskRunAttemptDispatcher::should_stay_queued`'s global cap counts against. Running
    /// is the only status that
    /// holds a slot, the same way `is_job_at_max_parallel_runs` counts only running job
    /// runs.
    pub async fn count_running_attempts(&self, conn: &mut SqliteConnection) -> anyhow::Result<u32> {

        let running_attempts = self.select_running_attempts(&mut *conn).await?;

        // A Running attempt with waiting_since set is asleep in one of flowlite's own wait
        // loops, holding a slot it is not using - see src/crud/multistatements/waiting.rs.
        // Bounding those is what deadlocks a pipeline that composes with `--wait`.
        let working = running_attempts
            .iter()
            .filter(|attempt| attempt.waiting_since.is_none())
            .count();

        Ok(working as u32)
    }

    /// How many running task run attempts currently claim each named limit - what
    /// `TaskRunAttemptDispatcher::a_claimed_limit_is_full` counts against. A name claimed by three
    /// Running attempts' task runs maps to `3`; a name nothing running claims is absent
    /// rather than `0`. Tallied from Running attempts only, the same as
    /// `count_running_attempts`.
    pub async fn claimed_limit_slots(&self, conn: &mut SqliteConnection) -> anyhow::Result<BTreeMap<String, u32>> {

        let running_attempts = self.select_running_attempts(&mut *conn).await?;

        let mut claimed_limit_slots = BTreeMap::new();

        for running_attempt in running_attempts.iter().filter(|attempt| attempt.waiting_since.is_none()) {

            let task_run = self.select_task_run(&mut *conn, &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: Some(running_attempt.task_run_id),
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            }).await?
                .ok_or_else(|| anyhow::anyhow!("Task run not found: {}", running_attempt.task_run_id))?;

            for limit in &task_run.limits.0 {
                *claimed_limit_slots.entry(limit.clone()).or_insert(0) += 1;
            }
        }

        Ok(claimed_limit_slots)
    }

    /// How many attempts are Running but asleep in one of flowlite's own waits. Holds no
    /// slot and appears in no limit, so it is invisible to every gate - which is exactly
    /// why the three surfaces that print the gates print this beside them, or a reader
    /// sees an idle-looking machine with thirty processes on it.
    pub async fn count_waiting_attempts(&self, conn: &mut SqliteConnection) -> anyhow::Result<u32> {

        let running_attempts = self.select_running_attempts(&mut *conn).await?;

        let waiting = running_attempts
            .iter()
            .filter(|attempt| attempt.waiting_since.is_some())
            .count();

        Ok(waiting as u32)
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::test_support::TestDb;

    /// The deadlock this exists to remove, in miniature: the attempt is Running and its
    /// process is alive, but it is asleep in a poll loop waiting on another run, so it is not
    /// what max_running_attempts is meant to bound.
    #[tokio::test]
    async fn a_waiting_attempt_is_not_counted_against_the_global_cap() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert_eq!(db.crud.count_running_attempts(&mut conn).await.unwrap(), 1);

        db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        assert_eq!(db.crud.count_running_attempts(&mut conn).await.unwrap(), 0);
    }

    /// Clearing puts it back: the wait returned, the command is working again, and it is once
    /// more the thing the cap is about.
    #[tokio::test]
    async fn clearing_the_mark_counts_the_attempt_again() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();
        db.crud.clear_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        assert_eq!(db.crud.count_running_attempts(&mut conn).await.unwrap(), 1);
    }

    /// The named limits go the same way, and for the same reason: a provider quota is about
    /// calls in flight, and a parent asleep on a child is making none.
    #[tokio::test]
    async fn a_waiting_attempt_releases_the_named_limits_it_claimed() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run_with_limits(job_run.id, vec!["openai_api".to_string()]).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert_eq!(db.crud.claimed_limit_slots(&mut conn).await.unwrap().get("openai_api"), Some(&1));

        db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        assert_eq!(db.crud.claimed_limit_slots(&mut conn).await.unwrap().get("openai_api"), None);
    }

    /// The count the three surfaces print beside the table, so a reader who sees 0 in use on a
    /// busy machine is told where the processes went.
    #[tokio::test]
    async fn waiting_attempts_are_counted_separately() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let waiting = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let other_task_run = db.insert_named_task_run(job_run.id, "worker", TaskRunStatus::Running).await;
        db.insert_task_run_attempt(&other_task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();

        assert_eq!(db.crud.count_waiting_attempts(&mut conn).await.unwrap(), 0);

        db.crud.mark_attempt_waiting(&mut conn, waiting.id).await.unwrap();

        assert_eq!(db.crud.count_waiting_attempts(&mut conn).await.unwrap(), 1);
        assert_eq!(db.crud.count_running_attempts(&mut conn).await.unwrap(), 1);
    }

    /// A settled attempt is nobody's slot and nobody's waiter, mark or no mark. Nothing clears
    /// the stamp when an attempt finishes, so this is the case that makes that safe.
    #[tokio::test]
    async fn a_settled_attempt_with_a_stale_mark_is_in_neither_count() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        db.crud.mark_attempt_waiting(&mut conn, attempt.id).await.unwrap();

        db.settle_task_run_attempt(attempt.id, TaskRunAttemptStatus::Succeeded).await;

        assert_eq!(db.crud.count_running_attempts(&mut conn).await.unwrap(), 0);
        assert_eq!(db.crud.count_waiting_attempts(&mut conn).await.unwrap(), 0);
    }
}
