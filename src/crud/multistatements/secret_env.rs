//! The startup check that refuses a `secret_env:` naming a secret nothing defines, and
//! the pure policy underneath it.

use std::collections::BTreeMap;
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter, SelectJobsDataSort};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};

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
/// task's, and each in the row_id order `CRUD::init` assigned, so "first" means the first
/// one a person reading the YAML top to bottom would reach.
///
/// Pure and database-free on purpose: `check_secret_env_is_satisfied` reads real
/// `mem.job`/`mem.task` rows, and seeding `mem` inside a test to exercise this logic would
/// race every other test's pooled connection over `mem`'s shared-cache schema lock - the
/// "database schema is locked: mem" hazard `TestDb`'s doc comment documents and Task 3
/// already hit. So every case of the policy belongs here, where it needs no seeding at all.
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
/// The job's YAML path is not on the `mem.job` row `check_secret_env_is_satisfied` reads
/// this from, so unlike the design's own example this message names only the job and the
/// task, not the file - carrying the path through would mean putting it onto that row for a
/// check that only ever runs once, at startup.
fn unsatisfied_secret_reference_error(reference: &SecretEnvReference) -> anyhow::Error {

    let declarer = match reference.task_id {
        Some(task_id) => format!("task '{}'", task_id),
        None => format!("job '{}'", reference.job_id),
    };

    anyhow::anyhow!(
        "{} needs secret '{}' for {}, but nothing defines it. Add it under [secrets] in \
         config.toml, or set FLOWLITE_SECRETS__{}.",
        declarer,
        reference.secret_name,
        reference.variable_name,
        reference.secret_name.to_ascii_uppercase(),
    ).context(format!("Job '{}'", reference.job_id))
}

impl CRUD {

    /// Refuses when a job's or a task's `secret_env:` names a secret `secrets` does not
    /// define - the check that keeps the spawn-time bail in `build_task_run_attempt_env`
    /// off the path of anything this process serves. Not off it entirely: this reads the
    /// declarations `init` seeded into `mem` at startup, so a rerun replaying a row whose
    /// YAML has since changed, or a run `job submit` created in another process from a
    /// YAML this server never read, still reaches that bail - which is why it names the
    /// job and the task and carries this message's remedy rather than deferring to it.
    /// Called from `serve.rs` alone, after `init` and before the bind: see the comment at
    /// that call site for why it cannot live in `init` itself.
    ///
    /// Deliberately conservative about the other thing `submit_job` knows and this does
    /// not: `job_run_task_definition` evicts a job-level `secret_env` name from the merged
    /// map when a task declares that same name in its own `env`, so a job whose every task
    /// overrides `secret_env: {PW: x}` that way submits no row referencing `x` at all -
    /// and this still refuses to start over it. That asymmetry is chosen, not overlooked.
    /// Modelling the eviction would mean duplicating the merge here, so the two places
    /// that reason about these declarations could then disagree about which references are
    /// live - and the failure mode of disagreeing is a job that starts and bails at 03:00,
    /// which is the outcome this check exists to prevent. Refusing a declaration nothing
    /// would have read costs one YAML edit, at startup, with the job and the name in the
    /// message; the remedy is to drop the `secret_env` entry no task uses.
    ///
    /// Its own coverage is the integration test in `tests/serve_secret_check.rs`, not a
    /// unit test: collecting real declarations here means reading real `mem.job`/
    /// `mem.task` rows, which needs `init` to have seeded `mem` first, and `mem` is one
    /// shared-cache name for the whole test binary (see `TestDb`) - seeding
    /// it from a test races every other test's pooled connection over its schema lock. The
    /// policy this delegates to, `first_unsatisfied_secret_reference`, carries the
    /// exhaustive unit coverage instead; this function does only a query each, a collect,
    /// and the delegate.
    /// `job_id` scopes the check. `serve` passes `None` and asks about the whole data
    /// directory, which is what it is about to run. `job submit -f` passes its own id and
    /// must not ask about anything else: by then this same `mem` also holds every installed
    /// job, and an installed job's unsatisfied secret is no reason to refuse a submit that
    /// has nothing to do with it.
    pub async fn check_secret_env_is_satisfied(
        &self,
        conn: &mut SqliteConnection,
        secrets: &BTreeMap<String, String>,
        job_id: Option<&str>,
    ) -> anyhow::Result<()> {

        let jobs = self.select_jobs(&mut *conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: job_id.map(str::to_string),
                name_like: None,
            },
            sort: Some(SelectJobsDataSort::RowId),
            limit: None,
            offset: None,
        }).await?;

        let tasks = self.select_tasks(&mut *conn, &SelectTasksData {
            filter: SelectTasksDataFilter {
                task_id: None,
                job_id: job_id.map(str::to_string),
            },
            sort: Some(SelectTasksDataSort::RowId),
            limit: None,
            offset: None,
        }).await?;

        let mut references: Vec<SecretEnvReference> = Vec::new();

        for job in jobs.iter() {
            for (variable_name, secret_name) in job.secret_env.0.iter() {
                references.push(SecretEnvReference {
                    job_id: &job.job_id,
                    task_id: None,
                    variable_name,
                    secret_name,
                });
            }
        }

        for task in tasks.iter() {
            for (variable_name, secret_name) in task.secret_env.0.iter() {
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
        assert!(error.contains("[secrets] in config.toml"), "{error}");
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
}
