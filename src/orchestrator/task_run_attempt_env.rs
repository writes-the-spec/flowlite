use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use crate::crud::job_run::JobRun;
use crate::crud::task_run::TaskRun;
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::run_dir::task_output_path;


/// The environment variables one attempt's command runs with, over the environment
/// flowlite itself inherited - `Command::envs` adds to that rather than replacing it, so
/// this map is an overlay and never the whole environment.
///
/// The one part of that inherited environment a command does not get is the `FLOWLITE_*`
/// namespace, which the dispatcher strips before applying this overlay. So every
/// `FLOWLITE_` variable a command sees is one this function put there, and nothing here
/// can be defeated by what the server happened to be started with.
///
/// The four layers are applied in the order the design fixes: the task's own env:, then
/// its resolved secrets, then the run's parameters, then the run metadata. A secret comes
/// after env: so a plain value can never shadow a credential, and metadata is last so
/// nothing a user writes can make a command lie about which run it belongs to. The result
/// channel - `FLOWLITE_TASK_OUTPUT` and one `FLOWLITE_INPUT_*` per dependency - is part of
/// that last layer, for the same reason: a task must not be able to forge where its own
/// result goes or where another task's came from.
///
/// `inputs` is the path of each dependency's result, keyed by that dependency's task id,
/// and holds an entry only for a dependency that produced one - so a task asks whether it
/// got a result by asking whether the variable is set.
///
/// `previous_attempt` is `Some` only for a retry, and names the files of the attempt before
/// it: `FLOWLITE_PREVIOUS_ATTEMPT_LOG` always, `FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT` only if
/// that attempt wrote a result.
///
/// `secrets` is the whole configured map, keyed by secret name rather than by variable
/// name - `task_run.secret_env` is the other half, variable name to secret name, and the
/// two are joined here. This is the one place in the codebase a secret's value exists
/// outside its own config, and only for as long as it takes to build this map for one
/// spawn.
pub fn build_task_run_attempt_env(
    task_run: &TaskRun,
    job_run: &JobRun,
    task_run_attempt: &TaskRunAttempt,
    data_dir: &str,
    job_run_dir: &Path,
    inputs: &BTreeMap<String, PathBuf>,
    previous_attempt: Option<&PreviousAttemptFiles>,
    secrets: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {

    let mut env = task_run.env.0.clone();

    for (name, secret_name) in task_run.secret_env.0.iter() {
        // A backstop, not the primary check: `serve` resolves every secret_env name
        // against the configured secrets at startup, so this cannot fire for a run whose
        // declaration that check saw. Two ordinary paths still reach it. A rerun replays
        // the old row's secret_env, so a `secret_env:` entry dropped from the YAML - and
        // its secret from the config - leaves a row naming a secret the startup check no
        // longer reads at all. And `job submit` from a separate process seeds its own
        // `mem` from the current YAML, so an entry added without restarting `serve` is
        // unknown to the running server's app_config.
        //
        // Nothing leaks either way: this returns before the spawn, so `set_to_running`
        // never writes started_at and the attempt stays Queued. But Queued is
        // re-selected on every poll pass, so this message is what an operator sees
        // repeating in the log until somebody acts on it - which is why it names the job
        // and the task rather than only the attempt, and carries the same remedy the
        // startup error does. The identical argument
        // `a_spawn_failure_names_the_task_and_the_working_dir` makes for a bad
        // working_dir.
        //
        // The remedy names FLOWLITE_SECRETS__<NAME>, reachable for every name
        // `is_valid_secret_name` allows - which is why that rule rejects `__`, a sequence
        // the env form reads as a nested key. Only a row snapshotted before that rule
        // could still name one, and naming the job and the task points at the YAML to fix
        // either way.
        let value = secrets.get(secret_name).ok_or_else(|| anyhow::anyhow!(
            "Job '{}' task '{}' needs secret '{}' for {}, but nothing defines it. Add it \
             under [secrets] in config.toml, or set FLOWLITE_SECRETS__{}.",
            task_run.job_id,
            task_run.task_id,
            secret_name,
            name,
            secret_name.to_ascii_uppercase(),
        ))?;

        env.insert(name.clone(), value.clone());
    }

    for (name, value) in job_run.parameters.0.iter() {
        env.insert(parameter_env_name(name), value.clone());
    }

    // Injected rather than inherited. The dispatcher strips every FLOWLITE_ variable the
    // child would have inherited, so a task command that calls flowlite itself gets the
    // directory this server is serving, for a stated reason instead of by accident.
    env.insert("FLOWLITE_DATA_DIR".to_string(), data_dir.to_string());

    env.insert("FLOWLITE_JOB_ID".to_string(), job_run.job_id.clone());
    env.insert("FLOWLITE_JOB_RUN_ID".to_string(), job_run.id.to_string());
    env.insert("FLOWLITE_TASK_ID".to_string(), task_run.task_id.clone());
    env.insert("FLOWLITE_TASK_RUN_ID".to_string(), task_run.id.to_string());
    env.insert("FLOWLITE_TASK_RUN_ATTEMPT_ID".to_string(), task_run_attempt.id.to_string());
    env.insert("FLOWLITE_ATTEMPT".to_string(), task_run_attempt.attempt.to_string());

    // Inserted unconditionally, which is also what stops a task's own `env:` forging it:
    // this write lands after the task's values and overwrites whatever was there.
    env.insert("FLOWLITE_SCHEDULED_AT".to_string(), job_run.scheduled_at.to_rfc3339());

    // Named for the attempt, not the task: a retry must not be handed the path the attempt
    // before it wrote to, or a retry that writes nothing inherits that result.
    env.insert(
        "FLOWLITE_TASK_OUTPUT".to_string(),
        task_output_path(job_run_dir, &task_run.task_id, task_run_attempt.attempt)
            .to_string_lossy()
            .into_owned(),
    );

    for (task_id, path) in inputs {
        env.insert(task_input_env_name(task_id), path.to_string_lossy().into_owned());
    }

    // Removed before they are set, since each is sometimes absent: a task's own `env:` must
    // not be able to hand it a previous attempt that never happened, or a result it never
    // wrote.
    env.remove("FLOWLITE_PREVIOUS_ATTEMPT_LOG");
    env.remove("FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT");

    if let Some(previous_attempt) = previous_attempt {
        env.insert(
            "FLOWLITE_PREVIOUS_ATTEMPT_LOG".to_string(),
            previous_attempt.log.to_string_lossy().into_owned(),
        );

        if let Some(output) = &previous_attempt.output {
            env.insert(
                "FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT".to_string(),
                output.to_string_lossy().into_owned(),
            );
        }
    }

    Ok(env)
}

/// What a retry is handed of the attempt before it, which failed - the only outcome
/// `TaskRunMonitor` retries.
pub struct PreviousAttemptFiles {
    pub log: PathBuf,
    pub output: Option<PathBuf>,
}

fn parameter_env_name(name: &str) -> String {
    format!("FLOWLITE_PARAM_{}", name.to_ascii_uppercase())
}

/// The variable a task reads one dependency's result path from.
///
/// A hyphen becomes an underscore because a variable name cannot hold one. That makes
/// `load-raw` and `load_raw` the same name, which the YAML layer refuses in one job - see
/// the copy of this function in `src/yaml_models/job_yaml.rs`, which is where a task id is
/// checked against what it will have to be here.
pub fn task_input_env_name(task_id: &str) -> String {
    format!("FLOWLITE_INPUT_{}", task_id.to_ascii_uppercase().replace('-', "_"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;

    /// The run directory the tests below build paths under - `job_run` is run 7, and
    /// `run_dir::job_run_dir` is the function that really derives this.
    fn run_dir() -> PathBuf {
        PathBuf::from("/srv/flowlite/.flowlite/runs/7")
    }

    fn no_inputs() -> BTreeMap<String, PathBuf> {
        BTreeMap::new()
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn job_run(parameters: BTreeMap<String, String>, scheduled_at: DateTime<Utc>) -> JobRun {
        JobRun {
            id: 7,
            job_id: "daily-etl".to_string(),
            job_name: "Daily ETL".to_string(),
            job_description: String::new(),
            parameters: sqlx::types::Json(parameters),
            created_at: Utc::now(),
            scheduled_at,
            schedule_id: None,
            started_at: None,
            finished_at: None,
            status: JobRunStatus::Running,
            parent_task_run_attempt_id: None,
        }
    }

    /// A due time for the tests that only need `job_run` to carry one, not care what it
    /// is.
    fn fixed_scheduled_at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-08T03:00:00Z").unwrap().with_timezone(&Utc)
    }

    fn task_run(env: BTreeMap<String, String>) -> TaskRun {
        TaskRun {
            id: 11,
            job_run_id: 7,
            job_id: "daily-etl".to_string(),
            task_id: "extract".to_string(),
            command: "true".to_string(),
            stdin: String::new(),
            depends_on: sqlx::types::Json(Vec::new()),
            limits: sqlx::types::Json(Vec::new()),
            timeout: 3600,
            max_retries: 0,
            retry_delay: 60,
            env: sqlx::types::Json(env),
            secret_env: sqlx::types::Json(BTreeMap::new()),
            working_dir: String::new(),
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            status: TaskRunStatus::Running,
        }
    }

    /// The fixture above plus a `secret_env` mapping, for the tests that resolve one.
    fn task_run_with_secret_env(env: BTreeMap<String, String>, secret_env: BTreeMap<String, String>) -> TaskRun {
        TaskRun {
            secret_env: sqlx::types::Json(secret_env),
            ..task_run(env)
        }
    }

    fn task_run_attempt() -> TaskRunAttempt {
        TaskRunAttempt {
            id: 13,
            task_run_id: 11,
            job_run_id: 7,
            job_id: "daily-etl".to_string(),
            task_id: "extract".to_string(),
            attempt: 2,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            status: TaskRunAttemptStatus::Queued,
            process_group_id: None,
            output: String::new(),
            waiting_since: None,
        }
    }

    #[test]
    fn a_task_env_value_is_carried() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[("PYTHONUNBUFFERED", "1")])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("PYTHONUNBUFFERED").unwrap(), "1");
    }

    /// The data directory is injected rather than inherited: the dispatcher strips every
    /// FLOWLITE_ variable it inherited, so a task command that calls flowlite itself would
    /// otherwise lose the directory it has to work on.
    #[test]
    fn the_data_dir_is_injected() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_DATA_DIR").unwrap(), "/srv/flowlite");
    }

    #[test]
    fn a_parameter_is_prefixed_and_upcased() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[("region", "us")]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_PARAM_REGION").unwrap(), "us");
    }

    /// The precedence the design fixes: a parameter is applied after the task's env:,
    /// so the two layers are ordered rather than racing.
    #[test]
    fn a_parameter_wins_over_a_colliding_task_env_value() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[("FLOWLITE_PARAM_REGION", "eu")])),
            &job_run(map(&[("region", "us")]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_PARAM_REGION").unwrap(), "us");
    }

    /// Metadata is applied last so nothing a user writes can make a command lie about
    /// which run it belongs to.
    #[test]
    fn injected_metadata_wins_over_a_task_env_value() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[("FLOWLITE_JOB_RUN_ID", "999")])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_JOB_RUN_ID").unwrap(), "7");
    }

    /// The parameter prefix keeps a parameter from ever colliding with an injected
    /// metadata key in the first place, so this does not exercise the layering order the
    /// way `injected_metadata_wins_over_a_task_env_value` does.
    #[test]
    fn a_parameter_cannot_collide_with_injected_metadata() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[("job_run_id", "999")]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_PARAM_JOB_RUN_ID").unwrap(), "999");
        assert_eq!(env.get("FLOWLITE_JOB_RUN_ID").unwrap(), "7");
    }

    #[test]
    fn every_id_and_the_attempt_are_injected() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_JOB_ID").unwrap(), "daily-etl");
        assert_eq!(env.get("FLOWLITE_JOB_RUN_ID").unwrap(), "7");
        assert_eq!(env.get("FLOWLITE_TASK_ID").unwrap(), "extract");
        assert_eq!(env.get("FLOWLITE_TASK_RUN_ID").unwrap(), "11");
        assert_eq!(env.get("FLOWLITE_TASK_RUN_ATTEMPT_ID").unwrap(), "13");
        assert_eq!(env.get("FLOWLITE_ATTEMPT").unwrap(), "2");
    }

    #[test]
    fn a_scheduled_run_carries_the_instant_it_fired_for() {
        let scheduled_at = DateTime::parse_from_rfc3339("2026-09-08T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), scheduled_at),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert!(env.get("FLOWLITE_SCHEDULED_AT").unwrap().starts_with("2026-09-08T03:00:00"));
    }

    /// Every run has a due time now, so every spawned command gets the variable. This
    /// inverts a rule that held while `scheduled_at` was nullable: back then a manual run
    /// had nothing honest to put here, and the key was removed rather than filled in.
    #[test]
    fn a_manual_run_carries_its_own_due_time() {
        let scheduled_at = DateTime::parse_from_rfc3339("2026-09-08T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), scheduled_at),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(
            env.get("FLOWLITE_SCHEDULED_AT").unwrap(),
            &scheduled_at.to_rfc3339(),
        );
    }

    /// A forged FLOWLITE_SCHEDULED_AT in a task's own env: does not survive - the metadata
    /// write lands after the task's own values and overwrites whatever was there, the same
    /// guarantee every other injected key already carries.
    #[test]
    fn a_task_env_value_for_scheduled_at_is_overwritten_by_the_runs_own_due_time() {
        let scheduled_at = fixed_scheduled_at();

        let env = build_task_run_attempt_env(
            &task_run(map(&[("FLOWLITE_SCHEDULED_AT", "1999-01-01T00:00:00Z")])),
            &job_run(map(&[]), scheduled_at),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(
            env.get("FLOWLITE_SCHEDULED_AT").unwrap(),
            &scheduled_at.to_rfc3339(),
        );
    }

    /// Named for the attempt rather than the task, so a retry cannot be handed the path
    /// the attempt before it wrote to.
    #[test]
    fn the_output_path_names_the_task_and_this_attempt() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(
            env.get("FLOWLITE_TASK_OUTPUT").unwrap(),
            "/srv/flowlite/.flowlite/runs/7/.output/extract.2",
        );
    }

    /// The result channel is part of the metadata layer, so a task cannot redirect where
    /// its own result is read from by declaring the name itself.
    #[test]
    fn a_task_env_value_for_the_output_path_is_overwritten() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[("FLOWLITE_TASK_OUTPUT", "/tmp/somewhere-else")])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(
            env.get("FLOWLITE_TASK_OUTPUT").unwrap(),
            "/srv/flowlite/.flowlite/runs/7/.output/extract.2",
        );
    }

    #[test]
    fn a_dependencys_result_is_injected_as_a_path_under_its_task_id() {
        let inputs = BTreeMap::from([
            ("plan".to_string(), PathBuf::from("/srv/flowlite/.flowlite/runs/7/.output/plan.1")),
        ]);

        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &inputs,
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(
            env.get("FLOWLITE_INPUT_PLAN").unwrap(),
            "/srv/flowlite/.flowlite/runs/7/.output/plan.1",
        );
    }

    /// A variable name cannot hold a hyphen, so the id is mapped rather than rejected
    /// here - the YAML layer is what refuses two task ids that would land on one name.
    #[test]
    fn a_hyphenated_dependency_becomes_an_underscored_variable() {
        let inputs = BTreeMap::from([
            ("load-raw".to_string(), PathBuf::from("/srv/flowlite/.flowlite/runs/7/.output/load-raw.1")),
        ]);

        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &inputs,
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert!(env.contains_key("FLOWLITE_INPUT_LOAD_RAW"), "{env:?}");
    }

    /// No inputs, no variables: a task asks whether it was given a result by asking
    /// whether the variable is set, so an empty map must not leave an empty path behind.
    #[test]
    fn a_task_with_no_inputs_is_given_no_input_variables() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap();

        assert!(
            !env.keys().any(|name| name.starts_with("FLOWLITE_INPUT_")),
            "{env:?}",
        );
    }

    #[test]
    fn a_secret_is_resolved_into_the_environment() {
        let env = build_task_run_attempt_env(
            &task_run_with_secret_env(map(&[]), map(&[("WAREHOUSE_PW", "warehouse_pw")])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &map(&[("warehouse_pw", "hunter2")]),
        ).unwrap();

        assert_eq!(env.get("WAREHOUSE_PW").unwrap(), "hunter2");
    }

    /// The layer order the design fixes: a resolved secret is applied after the task's own
    /// env:, so a plain env: value cannot shadow a credential.
    ///
    /// The row below cannot be submitted: `job_run_task_definition` evicts a name from one
    /// block when the task declares it in the other, so no `task_run` carries one name in
    /// both `env` and `secret_env`. It is built by hand for the same reason
    /// `injected_metadata_still_wins_a_colliding_secret` builds an impossible one - the
    /// order has to hold on its own rather than by the upstream invariant's leave. Read it
    /// as a property of this function, not as a live collision wanting shadowing logic
    /// here; that belongs where the two blocks are merged.
    #[test]
    fn a_secret_wins_a_colliding_env_value() {
        let env = build_task_run_attempt_env(
            &task_run_with_secret_env(
                map(&[("WAREHOUSE_PW", "not_a_secret")]),
                map(&[("WAREHOUSE_PW", "warehouse_pw")]),
            ),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &map(&[("warehouse_pw", "hunter2")]),
        ).unwrap();

        assert_eq!(env.get("WAREHOUSE_PW").unwrap(), "hunter2");
    }

    /// The YAML layer rejects a `secret_env` name starting with `FLOWLITE_`, so this
    /// collision cannot arise from a real job - but the layering order still has to hold
    /// under it, since injected metadata is what the whole map is applied over.
    #[test]
    fn injected_metadata_still_wins_a_colliding_secret() {
        let env = build_task_run_attempt_env(
            &task_run_with_secret_env(map(&[]), map(&[("FLOWLITE_JOB_RUN_ID", "warehouse_pw")])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &map(&[("warehouse_pw", "hunter2")]),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_JOB_RUN_ID").unwrap(), "7");
    }

    /// A backstop for what the startup check in `serve` did not see: a rerun of a row
    /// whose YAML has since dropped the entry, or a run another process submitted from a
    /// YAML this server has not read. Neither leaks - `set_to_running` bails before
    /// writing `started_at` - but the attempt stays Queued and is re-selected every poll
    /// pass, so this message is printed for ever until somebody acts on it. It therefore
    /// has to carry everything acting on it needs: which job and task, which variable and
    /// secret, and what to do about it. The same argument
    /// `a_spawn_failure_names_the_task_and_the_working_dir` makes for a bad working_dir.
    #[test]
    fn a_secret_with_no_value_names_the_job_the_task_the_variable_the_secret_and_the_remedy() {
        let error = build_task_run_attempt_env(
            &task_run_with_secret_env(map(&[]), map(&[("WAREHOUSE_PW", "warehouse_pw")])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &BTreeMap::new(),
        ).unwrap_err().to_string();

        assert!(error.contains("daily-etl"), "{error}");
        assert!(error.contains("extract"), "{error}");
        assert!(error.contains("WAREHOUSE_PW"), "{error}");
        assert!(error.contains("warehouse_pw"), "{error}");
        assert!(error.contains("[secrets] in config.toml"), "{error}");
        assert!(error.contains("FLOWLITE_SECRETS__WAREHOUSE_PW"), "{error}");
    }

    #[test]
    fn a_first_attempt_is_handed_no_previous_attempt() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            None,
            &map(&[]),
        ).unwrap();

        assert!(!env.contains_key("FLOWLITE_PREVIOUS_ATTEMPT_LOG"));
        assert!(!env.contains_key("FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT"));
    }

    #[test]
    fn a_retry_is_handed_the_previous_attempts_log_and_result() {
        let previous_attempt = PreviousAttemptFiles {
            log: run_dir().join(".output/extract.1.log"),
            output: Some(run_dir().join(".output/extract.1")),
        };

        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            Some(&previous_attempt),
            &map(&[]),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_PREVIOUS_ATTEMPT_LOG").unwrap(), "/srv/flowlite/.flowlite/runs/7/.output/extract.1.log");
        assert_eq!(env.get("FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT").unwrap(), "/srv/flowlite/.flowlite/runs/7/.output/extract.1");
    }

    /// A task's own `env:` cannot plant a previous attempt that never happened.
    #[test]
    fn a_task_cannot_forge_a_previous_attempt() {
        let previous_attempt = PreviousAttemptFiles {
            log: run_dir().join(".output/extract.1.log"),
            output: None,
        };

        let env = build_task_run_attempt_env(
            &task_run(map(&[
                ("FLOWLITE_PREVIOUS_ATTEMPT_LOG", "/tmp/forged"),
                ("FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT", "/tmp/forged"),
            ])),
            &job_run(map(&[]), fixed_scheduled_at()),
            &task_run_attempt(),
            "/srv/flowlite",
            &run_dir(),
            &no_inputs(),
            Some(&previous_attempt),
            &map(&[]),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_PREVIOUS_ATTEMPT_LOG").unwrap(), "/srv/flowlite/.flowlite/runs/7/.output/extract.1.log");
        assert!(!env.contains_key("FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT"));
    }
}
