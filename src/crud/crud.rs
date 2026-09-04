use std::sync::Arc;
use anyhow::Context;
use sqlx::Acquire;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::fs;
use crate::crud::job::{InsertJobData, InsertJobDataInput};
use crate::crud::schedule::{InsertScheduleData, InsertScheduleDataInput};
use crate::crud::schedule_job::{InsertScheduleJobData, InsertScheduleJobDataInput};
use crate::crud::task::{InsertTaskData, InsertTaskDataInput};
use crate::crud::task_dependent::{InsertTaskDependentData, InsertTaskDependentDataInput};
use crate::toolkit::Toolkit;
use crate::yaml_models::job_yaml::JobYaml;
use crate::yaml_models::schedule_yaml::ScheduleYaml;
use crate::cron_trigger::CronTrigger;


#[derive(Clone)]
pub struct CRUD {
    pub toolkit: Arc<Toolkit>,
}


impl CRUD {

    pub fn new(
        toolkit: Arc<Toolkit>,
    ) -> Self {
        Self { toolkit }
    }

    pub async fn init<'e, E>(&self, executor: E) -> anyhow::Result<()>
    where
        E: Acquire<'e, Database = sqlx::Sqlite>,
    {
        let config_dir = PathBuf::from(&self.toolkit.app_config.config_dir);

        let mut conn = executor.acquire().await
            .context("Failed to acquire a database connection to read the configuration into")?;

        let mut tx = conn.begin().await
            .context("Failed to begin the configuration transaction")?;

        let mut row_id = 0;

        let jobs_dir = config_dir.join("jobs");
        if jobs_dir.exists() {
            for job_path in CRUD::read_dir_sorted(&jobs_dir)? {
                if job_path.is_file() && job_path.extension().and_then(|s| s.to_str()) == Some("yml") {
                    let job_yaml = JobYaml::from_yaml(&job_path)?;

                    Self::validate_job_tasks(&job_yaml)
                        .with_context(|| format!(
                            "Invalid tasks of job '{}' at {}",
                            job_yaml.id,
                            job_path.display(),
                        ))?;

                    row_id += 1;
                    self.insert_job(&mut *tx, &InsertJobData {
                        input: InsertJobDataInput {
                            row_id,
                            job_id: job_yaml.id.clone(),
                            name: job_yaml.name,
                            description: job_yaml.description,
                            max_active_runs: job_yaml.max_active_runs,
                        }
                    })
                        .await
                        .with_context(|| format!(
                            "Failed to insert job '{}' from {}",
                            job_yaml.id,
                            job_path.display(),
                        ))?;

                    for task_yaml in job_yaml.tasks {
                        row_id += 1;
                        self.insert_task(&mut *tx, &InsertTaskData {
                            input: InsertTaskDataInput {
                                row_id,
                                task_id: task_yaml.id.clone(),
                                job_id: job_yaml.id.clone(),
                                command: task_yaml.command,
                                depends_on: task_yaml.depends_on.clone(),
                                timeout: task_yaml.timeout,
                                max_retries: task_yaml.max_retries,
                                retry_delay: task_yaml.retry_delay,
                            }
                        })
                            .await
                            .with_context(|| format!(
                                "Failed to insert task '{}' of job '{}' from {}",
                                task_yaml.id,
                                job_yaml.id,
                                job_path.display(),
                            ))?;

                        for dependent_task_id in task_yaml.depends_on {
                            row_id += 1;
                            self.insert_task_dependent(&mut *tx, &InsertTaskDependentData {
                                input: InsertTaskDependentDataInput {
                                    row_id,
                                    job_id: job_yaml.id.clone(),
                                    task_id: task_yaml.id.clone(),
                                    dependent_task_id: dependent_task_id.clone(),
                                }
                            })
                                .await
                                .with_context(|| format!(
                                    "Failed to insert dependency '{}' of task '{}' of job '{}' from {}",
                                    dependent_task_id,
                                    task_yaml.id,
                                    job_yaml.id,
                                    job_path.display(),
                                ))?;
                        }

                    }
                }
            }
        }

        let schedules_dir = config_dir.join("schedules");
        if schedules_dir.exists() {
            for schedule_path in CRUD::read_dir_sorted(&schedules_dir)? {
                if schedule_path.is_file() && schedule_path.extension().and_then(|s| s.to_str()) == Some("yml") {
                    let schedule_yaml = ScheduleYaml::from_yaml(&schedule_path)?;

                    let cron_trigger = CronTrigger::new(
                        schedule_yaml.cron.clone(),
                        schedule_yaml.timezone,
                        schedule_yaml.start_date,
                        schedule_yaml.end_date,
                    );

                    let next_run = cron_trigger.get_next_run(None);

                    row_id += 1;
                    self.insert_schedule(&mut *tx, &InsertScheduleData {
                        input: InsertScheduleDataInput {
                            row_id,
                            schedule_id: schedule_yaml.id.clone(),
                            name: schedule_yaml.name,
                            description: schedule_yaml.description,
                            cron: schedule_yaml.cron,
                            timezone: schedule_yaml.timezone,
                            start_date: schedule_yaml.start_date,
                            end_date: schedule_yaml.end_date,
                            disabled: schedule_yaml.disabled,
                            next_run,
                        }
                    })
                        .await
                        .with_context(|| format!(
                            "Failed to insert schedule '{}' from {}",
                            schedule_yaml.id,
                            schedule_path.display(),
                        ))?;

                    for schedule_job_yaml in schedule_yaml.jobs {
                        row_id += 1;
                        self.insert_schedule_job(&mut *tx, &InsertScheduleJobData {
                            input: InsertScheduleJobDataInput {
                                row_id,
                                schedule_id: schedule_yaml.id.clone(),
                                job_id: schedule_job_yaml.id.clone(),
                                parameters: schedule_job_yaml.parameters.map(|p| p.to_string()),
                            }
                        })
                            .await
                            .with_context(|| format!(
                                "Failed to insert job '{}' of schedule '{}' from {}",
                                schedule_job_yaml.id,
                                schedule_yaml.id,
                                schedule_path.display(),
                            ))?;
                    }
                    
                }
            }
        }

        tx.commit().await
            .with_context(|| format!(
                "Failed to commit the configuration read from {}",
                config_dir.display(),
            ))?;

        Ok(())
    }

    /// Rejects a task graph the orchestrator could never finish. TaskRunDispatcher only
    /// starts a task run once every task run it depends on has succeeded, so a dependency
    /// on a task that isn't part of the job, or a cycle between tasks, would leave the
    /// task runs pending - and their job run running - forever.
    fn validate_job_tasks(job_yaml: &JobYaml) -> anyhow::Result<()> {

        let mut task_ids: HashSet<&str> = HashSet::new();

        for task_yaml in job_yaml.tasks.iter() {
            if !task_ids.insert(task_yaml.id.as_str()) {
                anyhow::bail!("Task '{}' is declared more than once", task_yaml.id);
            }
        }

        for task_yaml in job_yaml.tasks.iter() {
            for dependent_task_id in task_yaml.depends_on.iter() {

                if dependent_task_id == &task_yaml.id {
                    anyhow::bail!("Task '{}' depends on itself", task_yaml.id);
                }

                if !task_ids.contains(dependent_task_id.as_str()) {
                    anyhow::bail!(
                        "Task '{}' depends on '{}', which is not a task of this job",
                        task_yaml.id,
                        dependent_task_id,
                    );
                }
            }
        }

        if let Some(cycle) = Self::find_task_dependency_cycle(job_yaml) {
            anyhow::bail!("Tasks depend on each other in a cycle: {}", cycle.join(" -> "));
        }

        Ok(())
    }

    /// Returns the tasks of the first dependency cycle, in the order they depend on each
    /// other, or None if the tasks form a directed acyclic graph.
    fn find_task_dependency_cycle(job_yaml: &JobYaml) -> Option<Vec<String>> {

        let depends_on_by_task_id: HashMap<&str, &[String]> = job_yaml.tasks
            .iter()
            .map(|task_yaml| (task_yaml.id.as_str(), task_yaml.depends_on.as_slice()))
            .collect();

        let mut acyclic_task_ids: HashSet<&str> = HashSet::new();
        let mut path: Vec<&str> = Vec::new();

        for task_yaml in job_yaml.tasks.iter() {

            let cycle = Self::walk_task_dependencies(
                task_yaml.id.as_str(),
                &depends_on_by_task_id,
                &mut acyclic_task_ids,
                &mut path,
            );

            if cycle.is_some() {
                return cycle;
            }
        }

        None
    }

    /// Walks the dependencies of one task depth first, reporting a cycle as soon as the
    /// path leads back to a task already on it. Tasks that turned out to be free of cycles
    /// are remembered, so no task is walked twice.
    fn walk_task_dependencies<'a>(
        task_id: &'a str,
        depends_on_by_task_id: &HashMap<&'a str, &'a [String]>,
        acyclic_task_ids: &mut HashSet<&'a str>,
        path: &mut Vec<&'a str>,
    ) -> Option<Vec<String>> {

        if let Some(position) = path.iter().position(|id| *id == task_id) {

            let mut cycle: Vec<String> = path[position..]
                .iter()
                .map(|id| id.to_string())
                .collect();

            cycle.push(task_id.to_string());

            return Some(cycle);
        }

        if acyclic_task_ids.contains(task_id) {
            return None;
        }

        path.push(task_id);

        let depends_on = depends_on_by_task_id
            .get(task_id)
            .copied()
            .unwrap_or(&[]);

        for dependent_task_id in depends_on.iter() {

            let cycle = Self::walk_task_dependencies(
                dependent_task_id.as_str(),
                depends_on_by_task_id,
                acyclic_task_ids,
                path,
            );

            if cycle.is_some() {
                return cycle;
            }
        }

        path.pop();
        acyclic_task_ids.insert(task_id);

        None
    }

    fn read_dir_sorted(path: impl AsRef<std::path::Path>) -> anyhow::Result<Vec<PathBuf>> {

        let path = path.as_ref();

        let entries = fs::read_dir(path)
            .with_context(|| format!("Failed to read directory {}", path.display()))?;

        let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
        paths.sort();

        Ok(paths)
    }

}
