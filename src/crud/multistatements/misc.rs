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

/// The parameters one run will carry: the job's declared defaults, with the caller's
/// overrides applied.
///
/// An override the job does not declare raises rather than being passed through. A
/// schedule or a --param naming a parameter that isn't there is a typo, and a typo that
/// delivers an unset variable to a command is the one outcome you cannot debug from the
/// row afterwards.
pub fn resolve_job_parameters(
    job_id: &str,
    declared: &BTreeMap<String, String>,
    overrides: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {

    let mut parameters = declared.clone();

    for (name, value) in overrides {

        if !declared.contains_key(name) {

            let declared_names = if declared.is_empty() {
                "none".to_string()
            } else {
                declared.keys().cloned().collect::<Vec<String>>().join(", ")
            };

            anyhow::bail!(
                "Job '{}' does not declare a parameter '{}'. Declared: {}",
                job_id,
                name,
                declared_names,
            );
        }

        parameters.insert(name.clone(), value.clone());
    }

    Ok(parameters)
}

/// Operations that span more than one entity, and so belong to no single entity file.
impl CRUD {

    /// Submits a run of the job's current definition. The definition is snapshotted onto
    /// the run's own rows, so what the run executes can no longer change under it -
    /// not when the YAML is edited, and not when the process restarts mid-run.
    ///
    /// `overrides` are the caller's parameter values, checked against what the job
    /// declares. `scheduled_at` is the instant a schedule fired for, and None for a
    /// manual submission - a manual run has no scheduled instant, and the spawned command
    /// gets no FLOWLITE_SCHEDULED_AT rather than a misleading copy of created_at.
    ///
    /// A job with no config is an error rather than an empty run: the caller asked for a
    /// job that isn't there.
    pub async fn submit_job(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
        overrides: &BTreeMap<String, String>,
        scheduled_at: Option<DateTime<Utc>>,
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

        let parameters = resolve_job_parameters(job_id, &job.parameters.0, overrides)?;

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
            parameters,
            scheduled_at,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn a_declared_default_is_carried_when_nothing_overrides_it() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region", "eu")]),
            &map(&[]),
        ).unwrap();

        assert_eq!(resolved.get("region").unwrap(), "eu");
    }

    #[test]
    fn an_override_replaces_the_default() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region", "eu")]),
            &map(&[("region", "us")]),
        ).unwrap();

        assert_eq!(resolved.get("region").unwrap(), "us");
    }

    #[test]
    fn the_parameters_the_override_does_not_mention_keep_their_defaults() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region", "eu"), ("slice", "")]),
            &map(&[("region", "us")]),
        ).unwrap();

        assert_eq!(resolved.get("slice").unwrap(), "");
        assert_eq!(resolved.len(), 2);
    }

    /// The whole reason declaration is worth having: a typo in a schedule is reported
    /// rather than silently delivering nothing to the command.
    #[test]
    fn an_undeclared_override_raises_naming_the_job_the_key_and_the_declared_names() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("region", "eu"), ("slice", "")]),
            &map(&[("regoin", "us")]),
        ).unwrap_err().to_string();

        assert!(error.contains("daily-etl"), "{error}");
        assert!(error.contains("regoin"), "{error}");
        assert!(error.contains("region"), "{error}");
        assert!(error.contains("slice"), "{error}");
    }

    #[test]
    fn an_override_of_a_job_declaring_nothing_says_so() {
        let error = resolve_job_parameters(
            "job",
            &map(&[]),
            &map(&[("region", "us")]),
        ).unwrap_err().to_string();

        assert!(error.contains("none"), "{error}");
    }

    #[test]
    fn a_job_declaring_nothing_resolves_to_nothing() {
        assert!(resolve_job_parameters("job", &map(&[]), &map(&[])).unwrap().is_empty());
    }

    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::test_support::TestDb;

    /// A rerun replays the run's own inputs. Nothing here reads config, so the rerun of a
    /// scheduled run stays a run for the same slice and the same instant - rerunning
    /// yesterday's failed daily job reruns it for yesterday.
    #[tokio::test]
    async fn a_rerun_replays_the_original_parameters_env_and_scheduled_at() {

        let db = TestDb::new().await;

        let scheduled_at = chrono::Utc::now() - chrono::TimeDelta::days(1);

        let job_run = db.insert_job_run_with_parameters(
            JobRunStatus::Failed,
            map(&[("region", "us")]),
            Some(scheduled_at),
        ).await;

        db.insert_task_run_for_command_with_env(
            job_run.id,
            "echo hi",
            map(&[("PYTHONUNBUFFERED", "1")]),
            "/tmp",
        ).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let rerun_id = db.crud.rerun_job(&mut conn, job_run.id).await.unwrap();

        let rerun = db.job_run(rerun_id).await;

        assert_eq!(rerun.parameters.0.get("region").unwrap(), "us");
        assert_eq!(
            rerun.scheduled_at.unwrap().timestamp_millis(),
            scheduled_at.timestamp_millis(),
        );

        let task_runs = db.crud.select_task_runs(
            &*db.conn_pool,
            &crate::crud::task_run::SelectTaskRunsData {
                filter: crate::crud::task_run::SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(rerun_id),
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            },
        ).await.unwrap();

        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].env.0.get("PYTHONUNBUFFERED").unwrap(), "1");
        assert_eq!(task_runs[0].working_dir, "/tmp");
        assert_eq!(task_runs[0].status, TaskRunStatus::Pending);
    }
}
