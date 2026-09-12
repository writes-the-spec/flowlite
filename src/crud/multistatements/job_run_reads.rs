//! The two read-assemblies more than one caller answers a question with: a run with its
//! task runs, and a run's attempts with what each wrote.

use std::collections::HashMap;
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt};
use crate::crud::task_run_attempt_output::{group_task_run_attempt_output, SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, SelectTaskRunAttemptOutputsDataSort, TaskRunAttemptOutputStreams};

impl CRUD {

    /// The run and every task run under it — what `job-run get` and the MCP `get_job_run`
    /// tool both assemble to answer "what happened to this run": one row from `job_run`
    /// and every row from `task_run` naming it.
    pub async fn select_job_run_with_task_runs(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
    ) -> anyhow::Result<(JobRun, Vec<TaskRun>)> {

        let job_run = self.select_job_run(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter { id: Some(job_run_id), job_id: None, status: None },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        let Some(job_run) = job_run else {
            anyhow::bail!("Job run {} not found", job_run_id);
        };

        let task_runs = self.select_task_runs(&mut *conn, &SelectTaskRunsData {
            filter: SelectTaskRunsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
                job_id: None,
                task_id: None,
                status: None,
            },
            sort: Some(SelectTaskRunsDataSort::Id),
        }).await?;

        Ok((job_run, task_runs))
    }

    /// Every task run attempt of a run, grouped with what each wrote to stdout and
    /// stderr — what `job-run logs` and the MCP `get_task_output` tool both assemble.
    /// `task_id` narrows to one task's attempts, exactly as `job-run logs --task` does.
    ///
    /// Checked against `job_run` first so an id nothing matches is reported as that,
    /// rather than as an empty run indistinguishable from one with no attempts yet.
    pub async fn select_task_run_attempt_logs(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
        task_id: Option<&str>,
    ) -> anyhow::Result<(Vec<TaskRunAttempt>, HashMap<i64, TaskRunAttemptOutputStreams>)> {

        let job_run = self.select_job_run(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter { id: Some(job_run_id), job_id: None, status: None },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        if job_run.is_none() {
            anyhow::bail!("Job run {} not found", job_run_id);
        }

        let task_run_attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: Some(job_run_id),
                task_id: task_id.map(str::to_string),
                status: None,
            },
            sort: Some(SelectTaskRunAttemptsDataSort::Id),
        }).await?;

        let task_run_attempt_output = self.select_task_run_attempt_outputs(&mut *conn, &SelectTaskRunAttemptOutputsData {
            filter: SelectTaskRunAttemptOutputsDataFilter {
                id: None,
                task_run_attempt_id: None,
                task_run_id: None,
                job_run_id: Some(job_run_id),
                job_id: None,
                task_id: task_id.map(str::to_string),
                stream: None,
            },
            sort: Some(SelectTaskRunAttemptOutputsDataSort::Id),
        }).await?;

        Ok((task_run_attempts, group_task_run_attempt_output(task_run_attempt_output)))
    }
}
