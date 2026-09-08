use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRunStatus};


/// A job's definition, as one job run will execute it. `submit_job` builds it from the
/// config the YAML declares now and `rerun_job` from an earlier run's snapshot, and the
/// insert treats both alike: where a definition came from is the caller's business.
struct JobRunDefinition {
    job_id: String,
    job_name: String,
    job_description: String,
    parameters: BTreeMap<String, String>,
    scheduled_at: Option<DateTime<Utc>>,
    tasks: Vec<JobRunTaskDefinition>,
}

struct JobRunTaskDefinition {
    task_id: String,
    command: String,
    depends_on: Vec<String>,
    timeout: u32,
    max_retries: u32,
    retry_delay: u32,
    env: BTreeMap<String, String>,
    working_dir: String,
}

/// Operations that span more than one entity, and so belong to no single entity file.
impl CRUD {

    /// Submits a run of the job's current definition. The definition is snapshotted onto
    /// the run's own rows, so what the run executes can no longer change under it -
    /// not when the YAML is edited, and not when the process restarts mid-run.
    ///
    /// A job with no config is an error rather than an empty run: the caller asked for a
    /// job that isn't there.
    pub async fn submit_job(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
    ) -> anyhow::Result<i64> {

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
            anyhow::bail!("Job '{}' not found", job_id);
        };

        let tasks = self.select_tasks(&mut *conn, &SelectTasksData {
            filter: SelectTasksDataFilter {
                task_id: None,
                job_id: Some(job_id.to_string()),
            },
            sort: Some(SelectTasksDataSort::RowId),
            limit: None,
            offset: None,
        }).await?;

        let definition = JobRunDefinition {
            job_id: job.job_id,
            job_name: job.name,
            job_description: job.description,
            parameters: BTreeMap::new(),
            scheduled_at: None,
            tasks: tasks
                .into_iter()
                .map(|task| JobRunTaskDefinition {
                    task_id: task.task_id,
                    command: task.command,
                    depends_on: task.depends_on.0,
                    timeout: task.timeout,
                    max_retries: task.max_retries,
                    retry_delay: task.retry_delay,
                    env: task.env.0.clone(),
                    working_dir: task.working_dir.clone(),
                })
                .collect(),
        };

        self.insert_job_run_definition(&mut *conn, &definition).await
    }

    /// Inserts a pending job run and one pending task run per task. This is the only
    /// place a run's config is written.
    async fn insert_job_run_definition(
        &self,
        conn: &mut SqliteConnection,
        definition: &JobRunDefinition,
    ) -> anyhow::Result<i64> {

        let job_run_id = self.insert_job_run(
            &mut *conn,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: definition.job_id.clone(),
                    job_name: definition.job_name.clone(),
                    job_description: definition.job_description.clone(),
                    parameters: definition.parameters.clone(),
                    scheduled_at: definition.scheduled_at,
                    status: JobRunStatus::Pending,
                }
            }
        ).await?;

        for task in definition.tasks.iter() {
            self.insert_task_run(
                &mut *conn,
                &InsertTaskRunData {
                    input: InsertTaskRunDataInput {
                        job_run_id,
                        job_id: definition.job_id.clone(),
                        task_id: task.task_id.clone(),
                        command: task.command.clone(),
                        depends_on: task.depends_on.clone(),
                        timeout: task.timeout,
                        max_retries: task.max_retries,
                        retry_delay: task.retry_delay,
                        env: task.env.clone(),
                        working_dir: task.working_dir.clone(),
                        status: TaskRunStatus::Pending,
                    }
                }
            ).await?;
        }

        Ok(job_run_id)
    }

    /// Submits a fresh run of the job the given run belongs to, whatever state that run
    /// is in. What gets run is the definition that run executed, not whatever the YAML
    /// says now - so a rerun of an old run is a rerun of the old config. Nothing here
    /// reads the config at all, which is why a run whose job YAML has since been deleted
    /// is still rerunnable.
    pub async fn rerun_job(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
    ) -> anyhow::Result<i64> {

        let job_run = self.select_job_run(
            &mut *conn,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: Some(job_run_id),
                    job_id: None,
                    status: None,
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        ).await?;

        let Some(job_run) = job_run else {
            anyhow::bail!("Job run {} not found", job_run_id);
        };

        let task_runs = self.select_task_runs(
            &mut *conn,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run_id),
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunsDataSort::Id),
            }
        ).await?;

        let definition = JobRunDefinition {
            job_id: job_run.job_id,
            job_name: job_run.job_name,
            job_description: job_run.job_description,
            parameters: job_run.parameters.0.clone(),
            scheduled_at: job_run.scheduled_at,
            tasks: task_runs
                .into_iter()
                .map(|task_run| JobRunTaskDefinition {
                    task_id: task_run.task_id,
                    command: task_run.command,
                    depends_on: task_run.depends_on.0,
                    timeout: task_run.timeout,
                    max_retries: task_run.max_retries,
                    retry_delay: task_run.retry_delay,
                    env: task_run.env.0.clone(),
                    working_dir: task_run.working_dir.clone(),
                })
                .collect(),
        };

        self.insert_job_run_definition(&mut *conn, &definition).await
    }

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

}
