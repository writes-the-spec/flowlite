//! The startup check that refuses a `secret_env:` naming a secret nothing defines, and
//! the pure policy underneath it.

use std::collections::BTreeMap;

use super::declared::Declared;

/// One `secret_env:` entry a job or a task declared — the unit
/// `first_unsatisfied_secret_reference` checks against the secrets `serve` was actually
/// given. `task_id` is `None` for a job-level declaration, which names no task of its own;
/// a task-level one always carries its own `task_id`, whether or not the name is shared
/// with the job's own block.
struct SecretEnvReference<'a> {
    job_id: &'a str,
    task_id: Option<&'a str>,
    variable_name: &'a str,
    secret_name: &'a str,
}

/// The first reference the given secrets do not satisfy, in the order the caller collected
/// them - `check_secret_env_is_satisfied` collects every job's declaration before any
/// task's, and each in reading order, so "first" means the first one a person reading the
/// YAML top to bottom would reach.
fn first_unsatisfied_secret_reference<'a, 'b>(
    references: &'b [SecretEnvReference<'a>],
    secrets: &BTreeMap<String, String>,
) -> Option<&'b SecretEnvReference<'a>> {

    references.iter().find(|reference| !secrets.contains_key(reference.secret_name))
}

/// Turns the first unsatisfied reference into the error `serve` reports. The job is the
/// outer context - so the message names the job even before the cause - and the reference
/// itself is the cause: a task's own declaration reads as "task '<id>'", and a job-level one
/// with no task of its own reads as "job '<id>'" rather than naming a task that isn't there.
///
/// The job's YAML path is not on `Job`, so this message names only the job and the task,
/// not the file.
fn unsatisfied_secret_reference_error(reference: &SecretEnvReference) -> anyhow::Error {

    let declarer = match reference.task_id {
        Some(task_id) => format!("task '{}'", task_id),
        None => format!("job '{}'", reference.job_id),
    };

    anyhow::anyhow!(
        "{} needs secret '{}' for {}, but nothing defines it. Add it under [secrets] in \
         flowlite.toml, or set FLOWLITE_SECRETS__{}.",
        declarer,
        reference.secret_name,
        reference.variable_name,
        reference.secret_name.to_ascii_uppercase(),
    ).context(format!("Job '{}'", reference.job_id))
}

impl Declared {

    /// Refuses when a job's or a task's `secret_env:` names a secret `secrets` does not
    /// define - the check that keeps the spawn-time bail in `build_task_run_attempt_env`
    /// off the path of anything `serve` runs. Not off it entirely: a rerun replaying a row
    /// whose YAML has since changed, or a run `job submit` created in another process from
    /// a YAML this server never read, still reaches that bail - which is why that bail
    /// names the job and the task and carries this message's remedy too.
    ///
    /// Deliberately conservative: `job_run_task_definition` evicts a job-level
    /// `secret_env` name from the merged map when a task declares that same name in its
    /// own `env`, and this still refuses over it. Modelling the eviction would duplicate
    /// the merge, and two places disagreeing about which references are live fails as a
    /// job that starts and bails at 03:00. Refusing costs one YAML edit, at startup.
    ///
    /// `job_id` scopes the check. `serve` passes `None` and asks about the whole data
    /// directory, which is what it is about to run. `job submit -f` passes its own id and
    /// must not ask about anything else: an installed job's unsatisfied secret is no reason
    /// to refuse a submit that has nothing to do with it.
    pub fn check_secret_env_is_satisfied(
        &self,
        secrets: &BTreeMap<String, String>,
        job_id: Option<&str>,
    ) -> anyhow::Result<()> {

        let in_scope = |declared_by: &str| job_id.is_none_or(|job_id| job_id == declared_by);

        let mut references: Vec<SecretEnvReference> = Vec::new();

        for job in self.jobs.iter().filter(|job| in_scope(&job.job_id)) {
            for (variable_name, secret_name) in job.secret_env.iter() {
                references.push(SecretEnvReference {
                    job_id: &job.job_id,
                    task_id: None,
                    variable_name,
                    secret_name,
                });
            }
        }

        for task in self.tasks.iter().filter(|task| in_scope(&task.job_id)) {
            for (variable_name, secret_name) in task.secret_env.iter() {
                references.push(SecretEnvReference {
                    job_id: &task.job_id,
                    task_id: Some(&task.task_id),
                    variable_name,
                    secret_name,
                });
            }
        }

        match first_unsatisfied_secret_reference(&references, secrets) {
            Some(reference) => Err(unsatisfied_secret_reference_error(reference)),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::map;

    fn task_reference<'a>(job_id: &'a str, task_id: &'a str, variable_name: &'a str, secret_name: &'a str) -> SecretEnvReference<'a> {
        SecretEnvReference { job_id, task_id: Some(task_id), variable_name, secret_name }
    }

    fn job_reference<'a>(job_id: &'a str, variable_name: &'a str, secret_name: &'a str) -> SecretEnvReference<'a> {
        SecretEnvReference { job_id, task_id: None, variable_name, secret_name }
    }

    #[test]
    fn no_references_at_all_is_satisfied() {
        assert!(first_unsatisfied_secret_reference(&[], &map(&[])).is_none());
    }

    #[test]
    fn every_reference_defined_is_satisfied() {
        let references = [
            task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw"),
            job_reference("nightly-sync", "API_KEY", "api_key"),
        ];

        let available = map(&[("warehouse_pw", "hunter2"), ("api_key", "abc")]);

        assert!(first_unsatisfied_secret_reference(&references, &available).is_none());
    }

    /// A task-level reference nothing defines is reported, naming its own job, task,
    /// variable and secret name - not just "something is wrong".
    #[test]
    fn an_undefined_task_level_reference_is_reported() {
        let references = [task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw")];

        let reference = first_unsatisfied_secret_reference(&references, &map(&[])).unwrap();

        assert_eq!(reference.job_id, "nightly-sync");
        assert_eq!(reference.task_id, Some("load"));
        assert_eq!(reference.variable_name, "PGPASSWORD");
        assert_eq!(reference.secret_name, "warehouse_pw");
    }

    /// The job-level case carries no task - `task_id` stays `None` rather than being
    /// attributed to a task that never declared it.
    #[test]
    fn an_undefined_job_level_reference_is_reported_with_no_task() {
        let references = [job_reference("nightly-sync", "API_KEY", "api_key")];

        let reference = first_unsatisfied_secret_reference(&references, &map(&[])).unwrap();

        assert_eq!(reference.job_id, "nightly-sync");
        assert!(reference.task_id.is_none());
        assert_eq!(reference.secret_name, "api_key");
    }

    /// "First" means first in the caller's order, not sorted some other way - a defined
    /// reference ahead of an undefined one must not hide behind it, and two undefined ones
    /// report the earlier.
    #[test]
    fn the_first_unsatisfied_reference_in_order_wins_over_a_later_one() {
        let references = [
            task_reference("nightly-sync", "extract", "API_KEY", "api_key"),
            task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw"),
        ];

        let available = map(&[("api_key", "abc")]);

        let reference = first_unsatisfied_secret_reference(&references, &available).unwrap();

        assert_eq!(reference.task_id, Some("load"));
        assert_eq!(reference.secret_name, "warehouse_pw");
    }

    #[test]
    fn a_reference_satisfied_by_an_empty_secret_value_still_counts_as_defined() {
        let references = [task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw")];

        // An empty string is still a defined secret - "nothing defines it" is about the
        // name being absent from the map, not about the value being non-empty.
        let available = map(&[("warehouse_pw", "")]);

        assert!(first_unsatisfied_secret_reference(&references, &available).is_none());
    }

    /// The error the design fixes, for the task-level case: the job as the outer context
    /// and "task '<id>' needs secret '<name>' for <variable>, but nothing defines it",
    /// naming the env var to set.
    #[test]
    fn a_task_level_error_names_the_job_the_task_the_variable_and_the_secret() {
        let reference = task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw");

        let error = format!("{:?}", unsatisfied_secret_reference_error(&reference));

        assert!(error.contains("Job 'nightly-sync'"), "{error}");
        assert!(error.contains("task 'load'"), "{error}");
        assert!(error.contains("secret 'warehouse_pw'"), "{error}");
        assert!(error.contains("PGPASSWORD"), "{error}");
        assert!(error.contains("but nothing defines it"), "{error}");
        assert!(error.contains("[secrets] in flowlite.toml"), "{error}");
        assert!(error.contains("FLOWLITE_SECRETS__WAREHOUSE_PW"), "{error}");
    }

    /// The job-level case names the job as the declarer too, rather than inventing a task
    /// that was never declared.
    #[test]
    fn a_job_level_error_names_the_job_rather_than_a_task() {
        let reference = job_reference("nightly-sync", "API_KEY", "api_key");

        let error = format!("{:?}", unsatisfied_secret_reference_error(&reference));

        assert!(error.contains("job 'nightly-sync' needs secret 'api_key'"), "{error}");
        assert!(!error.contains("task '"), "{error}");
        assert!(error.contains("FLOWLITE_SECRETS__API_KEY"), "{error}");
    }

    /// The env var name the message tells someone to set is the secret name uppercased,
    /// whatever case and punctuation the name itself carries within what Task 2 allows.
    #[test]
    fn the_suggested_environment_variable_is_the_secret_name_uppercased() {
        let reference = task_reference("job", "task", "VAR", "warehouse_read_only_pw");

        let error = format!("{:?}", unsatisfied_secret_reference_error(&reference));

        assert!(error.contains("FLOWLITE_SECRETS__WAREHOUSE_READ_ONLY_PW"), "{error}");
    }

    fn declared_with_secret_env(job_id: &str, task_id: &str, secret_name: &str) -> Declared {
        let yaml = format!("
id: {job_id}
name: Job
tasks:
  - id: {task_id}
    command: ./run.sh
    secret_env:
      PGPASSWORD: {secret_name}
");
        let mut declared = Declared::default();
        declared.add_job(
            &crate::app_config::AppConfig::default(),
            serde_yaml::from_str(&yaml).unwrap(),
            std::path::Path::new("job.yaml"),
        ).unwrap();
        declared
    }

    #[test]
    fn the_whole_data_directory_is_checked_when_no_job_is_named() {
        let declared = declared_with_secret_env("nightly-sync", "load", "warehouse_pw");

        let error = format!("{:?}", declared.check_secret_env_is_satisfied(&map(&[]), None).unwrap_err());

        assert!(error.contains("task 'load'"), "{error}");
    }

    /// `job submit -f` names its own job, and another job's missing secret is not its
    /// problem.
    #[test]
    fn a_job_named_in_scope_ignores_every_other_job() {
        let declared = declared_with_secret_env("nightly-sync", "load", "warehouse_pw");

        assert!(declared.check_secret_env_is_satisfied(&map(&[]), Some("ad-hoc")).is_ok());
        assert!(declared.check_secret_env_is_satisfied(&map(&[]), Some("nightly-sync")).is_err());
    }
}
