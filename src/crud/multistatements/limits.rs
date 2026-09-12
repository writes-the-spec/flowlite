//! What the orchestrator's two concurrency gates count against: runs in flight for one
//! job, and attempts running across every job. Only a Running row holds a slot.

use std::collections::BTreeMap;
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttemptStatus};

impl CRUD {

    /// Whether the job already has as many runs in flight as it allows. Only a running
    /// job run holds a slot — a pending one is waiting for exactly this answer — and a
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
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

        Ok(running_job_runs.len() >= job.max_parallel_runs as usize)
    }

    /// How many task run attempts are Running right now, across every job - what
    /// `settle_as_pending`'s global cap counts against. Running is the only status that
    /// holds a slot, the same way `is_job_at_max_parallel_runs` counts only running job
    /// runs.
    pub async fn count_running_attempts(&self, conn: &mut SqliteConnection) -> anyhow::Result<u32> {

        let running_attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: Some(TaskRunAttemptStatus::Running),
            },
            sort: None,
        }).await?;

        Ok(running_attempts.len() as u32)
    }

    /// How many running task run attempts currently claim each named limit - what
    /// `settle_as_pending`'s named-limit gate counts against. A name claimed by three
    /// Running attempts' task runs maps to `3`; a name nothing running claims is absent
    /// rather than `0`. Tallied from Running attempts only, the same as
    /// `count_running_attempts`.
    pub async fn claimed_limit_slots(&self, conn: &mut SqliteConnection) -> anyhow::Result<BTreeMap<String, u32>> {

        let running_attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: Some(TaskRunAttemptStatus::Running),
            },
            sort: None,
        }).await?;

        let mut claimed_limit_slots = BTreeMap::new();

        for running_attempt in &running_attempts {

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
}
