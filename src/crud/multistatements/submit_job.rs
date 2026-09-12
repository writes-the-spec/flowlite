//! `submit_job`: a run of the job's current definition, snapshotted onto the run's own
//! rows so that what the run executes can no longer change under it.

use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};

use super::job_run_definition::{job_run_notification_definitions, job_run_task_definition, JobRunDefinition};

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

    for name in parameters.keys() {
        if !is_valid_parameter_name(name) {
            anyhow::bail!(
                "Job '{}' has a parameter named '{}', which is not a valid environment \
                 variable name. A parameter name may contain only ASCII letters, digits \
                 and underscores, and may not start with a digit.",
                job_id,
                name,
            );
        }
    }

    // Two names that only differ in case become the same FLOWLITE_PARAM_ env var, and a
    // BTreeMap can only hold one of them - so this has to be caught here rather than left
    // to silently drop one value at spawn time.
    let mut seen_env_names: BTreeMap<String, &String> = BTreeMap::new();

    for name in parameters.keys() {
        let env_name = name.to_ascii_uppercase();

        if let Some(other_name) = seen_env_names.insert(env_name.clone(), name) {
            anyhow::bail!(
                "Job '{}' declares parameters '{}' and '{}', which both become the \
                 environment variable FLOWLITE_PARAM_{}. Parameter names must be distinct \
                 once uppercased.",
                job_id,
                other_name,
                name,
                env_name,
            );
        }
    }

    Ok(parameters)
}

fn is_valid_parameter_name(name: &str) -> bool {
    let mut chars = name.chars();

    let Some(first) = chars.next() else {
        return false;
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

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
            // Cloned rather than moved so the whole row is still borrowable below: each
            // task's definition is built against the job's own `env` and `secret_env`.
            job_id: job.job_id.clone(),
            job_name: job.name.clone(),
            job_description: job.description.clone(),
            parameters,
            scheduled_at,
            tasks: tasks
                .iter()
                .map(|task| job_run_task_definition(task, &job))
                .collect(),
            notifications: job_run_notification_definitions(
                &job.on_failure_recipients.0,
                &job.on_success_recipients.0,
            ),
        };

        self.insert_job_run_definition(&mut *conn, &definition).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::map;


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

    /// Hyphenated identifiers are this repo's own naming convention, so `my-param` is the
    /// natural thing for a user to declare - and it is exactly the name `sh` cannot read
    /// out of an env var.
    #[test]
    fn a_hyphenated_parameter_name_is_rejected() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("my-param", "v")]),
            &map(&[]),
        ).unwrap_err().to_string();

        assert!(error.contains("daily-etl"), "{error}");
        assert!(error.contains("my-param"), "{error}");
    }

    #[test]
    fn a_parameter_name_starting_with_a_digit_is_rejected() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("1region", "v")]),
            &map(&[]),
        ).unwrap_err().to_string();

        assert!(error.contains("1region"), "{error}");
    }

    #[test]
    fn a_parameter_name_with_underscores_and_digits_is_accepted() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region_2", "eu")]),
            &map(&[]),
        ).unwrap();

        assert_eq!(resolved.get("region_2").unwrap(), "eu");
    }

    /// `region` and `REGION` are distinct declared parameters but the same env var once
    /// uppercased, and one of them would otherwise silently lose its value.
    #[test]
    fn two_names_differing_only_in_case_are_rejected_as_a_collision() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("region", "eu"), ("REGION", "us")]),
            &map(&[]),
        ).unwrap_err().to_string();

        assert!(error.contains("daily-etl"), "{error}");
        assert!(error.contains("region"), "{error}");
        assert!(error.contains("REGION"), "{error}");
        assert!(error.contains("FLOWLITE_PARAM_REGION"), "{error}");
    }
}
