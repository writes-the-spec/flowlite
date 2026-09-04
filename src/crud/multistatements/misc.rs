use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, TaskRunStatus};


/// Operations that span more than one entity, and so belong to no single entity file.
impl CRUD {

    pub async fn submit_job(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
    ) -> anyhow::Result<i64> {

        let job_run_id = self.insert_job_run(
            &mut *conn,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: job_id.to_string(),
                    status: JobRunStatus::Pending,
                }
            }
        ).await?;

        let tasks = self.select_tasks(
            &mut *conn,
            &SelectTasksData {
                filter: SelectTasksDataFilter {
                    task_id: None,
                    job_id: Some(job_id.to_string()),
                },
                sort: Some(SelectTasksDataSort::RowId),
                limit: None,
                offset: None,
            }
        ).await?;

        for task in tasks {
            self.insert_task_run(
                &mut *conn,
                &InsertTaskRunData {
                    input: InsertTaskRunDataInput {
                        job_run_id,
                        job_id: task.job_id.clone(),
                        task_id: task.task_id.clone(),
                        status: TaskRunStatus::Pending,
                    }
                }
            ).await?;
        }

        Ok(job_run_id)
    }

    /// Submits a fresh run of the job the given run belongs to, whatever state that run
    /// is in. What gets run is the job's current definition, not the tasks the original
    /// run happened to have, so a rerun picks up any change to the job's YAML since.
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

        self.submit_job(conn, &job_run.job_id).await
    }

    /// Whether the job already has as many active runs as it allows, which is the
    /// question every caller has to answer before submitting it. A job run counts as
    /// active until it finishes, so a queued one holds a slot just like a running one,
    /// and a max_active_runs of 0 means the job has no limit at all.
    pub async fn is_job_at_max_active_runs(
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

        if job.max_active_runs == 0 {
            return Ok(false);
        }

        let active_job_runs = self.count_active_job_runs(&mut *conn, job_id).await?;

        Ok(active_job_runs >= job.max_active_runs as usize)
    }

    async fn count_active_job_runs(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
    ) -> anyhow::Result<usize> {

        let pending_job_runs = self.select_job_runs(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: Some(job_id.to_string()),
                status: Some(JobRunStatus::Pending),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

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

        Ok(pending_job_runs.len() + running_job_runs.len())
    }

}
