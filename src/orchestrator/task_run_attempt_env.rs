use std::collections::BTreeMap;
use crate::crud::job_run::JobRun;
use crate::crud::task_run::TaskRun;
use crate::crud::task_run_attempt::TaskRunAttempt;


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
/// nothing a user writes can make a command lie about which run it belongs to.
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
    secrets: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {

    let mut env = task_run.env.0.clone();

    for (name, secret_name) in task_run.secret_env.0.iter() {
        // A backstop, not the primary check: Task 5 adds a startup check in `serve` that
        // resolves every secret_env name against the configured secrets before a job can
        // be served at all, which makes this unreachable for a served job. It still has
        // to fail loudly here, naming both sides, for whatever reaches this function
        // without having gone through that check.
        let value = secrets.get(secret_name).ok_or_else(|| anyhow::anyhow!(
            "Task run attempt {} needs environment variable '{}' from secret '{}', but no \
             such secret is configured",
            task_run_attempt.id,
            name,
            secret_name,
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

    // A BTreeMap can't express "unset", so a manual run (no scheduled_at) removes the key
    // rather than leaving it absent from this map - otherwise a task env: value for this
    // exact name would survive into the composed map untouched.
    if let Some(scheduled_at) = job_run.scheduled_at {
        env.insert("FLOWLITE_SCHEDULED_AT".to_string(), scheduled_at.to_rfc3339());
    } else {
        env.remove("FLOWLITE_SCHEDULED_AT");
    }

    Ok(env)
}

fn parameter_env_name(name: &str) -> String {
    format!("FLOWLITE_PARAM_{}", name.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn job_run(parameters: BTreeMap<String, String>, scheduled_at: Option<DateTime<Utc>>) -> JobRun {
        JobRun {
            id: 7,
            job_id: "daily-etl".to_string(),
            job_name: "Daily ETL".to_string(),
            job_description: String::new(),
            parameters: sqlx::types::Json(parameters),
            created_at: Utc::now(),
            scheduled_at,
            started_at: None,
            finished_at: None,
            status: JobRunStatus::Running,
        }
    }

    fn task_run(env: BTreeMap<String, String>) -> TaskRun {
        TaskRun {
            id: 11,
            job_run_id: 7,
            job_id: "daily-etl".to_string(),
            task_id: "extract".to_string(),
            command: "true".to_string(),
            depends_on: sqlx::types::Json(Vec::new()),
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
            status: TaskRunAttemptStatus::Pending,
            process_group_id: None,
        }
    }

    #[test]
    fn a_task_env_value_is_carried() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[("PYTHONUNBUFFERED", "1")])),
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
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
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_DATA_DIR").unwrap(), "/srv/flowlite");
    }

    #[test]
    fn a_parameter_is_prefixed_and_upcased() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[("region", "us")]), None),
            &task_run_attempt(),
            "/srv/flowlite",
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
            &job_run(map(&[("region", "us")]), None),
            &task_run_attempt(),
            "/srv/flowlite",
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
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
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
            &job_run(map(&[("job_run_id", "999")]), None),
            &task_run_attempt(),
            "/srv/flowlite",
            &BTreeMap::new(),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_PARAM_JOB_RUN_ID").unwrap(), "999");
        assert_eq!(env.get("FLOWLITE_JOB_RUN_ID").unwrap(), "7");
    }

    #[test]
    fn every_id_and_the_attempt_are_injected() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
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
            &job_run(map(&[]), Some(scheduled_at)),
            &task_run_attempt(),
            "/srv/flowlite",
            &BTreeMap::new(),
        ).unwrap();

        assert!(env.get("FLOWLITE_SCHEDULED_AT").unwrap().starts_with("2026-09-08T03:00:00"));
    }

    /// Absent, not empty: a manual run has no scheduled instant, and a command that needs
    /// one should fail on an unset variable rather than process the wrong day.
    #[test]
    fn a_manual_run_carries_no_scheduled_at_at_all() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[])),
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
            &BTreeMap::new(),
        ).unwrap();

        assert!(!env.contains_key("FLOWLITE_SCHEDULED_AT"));
    }

    /// A forged FLOWLITE_SCHEDULED_AT in a task's own env: must not survive a manual run -
    /// otherwise a command reading it would silently process whatever date the task
    /// definition claims instead of failing on an unset variable.
    #[test]
    fn a_task_env_value_for_scheduled_at_does_not_survive_a_manual_run() {
        let env = build_task_run_attempt_env(
            &task_run(map(&[("FLOWLITE_SCHEDULED_AT", "1999-01-01T00:00:00Z")])),
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
            &BTreeMap::new(),
        ).unwrap();

        assert!(!env.contains_key("FLOWLITE_SCHEDULED_AT"));
    }

    #[test]
    fn a_secret_is_resolved_into_the_environment() {
        let env = build_task_run_attempt_env(
            &task_run_with_secret_env(map(&[]), map(&[("WAREHOUSE_PW", "warehouse_pw")])),
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
            &map(&[("warehouse_pw", "hunter2")]),
        ).unwrap();

        assert_eq!(env.get("WAREHOUSE_PW").unwrap(), "hunter2");
    }

    /// The layer order the design fixes: a resolved secret is applied after the task's own
    /// env:, so a plain env: value cannot shadow a credential.
    #[test]
    fn a_secret_wins_a_colliding_env_value() {
        let env = build_task_run_attempt_env(
            &task_run_with_secret_env(
                map(&[("WAREHOUSE_PW", "not_a_secret")]),
                map(&[("WAREHOUSE_PW", "warehouse_pw")]),
            ),
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
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
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
            &map(&[("warehouse_pw", "hunter2")]),
        ).unwrap();

        assert_eq!(env.get("FLOWLITE_JOB_RUN_ID").unwrap(), "7");
    }

    /// A backstop, not the primary check - Task 5 adds a startup check in `serve` that
    /// makes this unreachable for a served job. It still has to fail loudly here, naming
    /// both sides, for whatever reaches this function without having gone through it.
    #[test]
    fn a_secret_with_no_value_is_an_error_naming_both() {
        let error = build_task_run_attempt_env(
            &task_run_with_secret_env(map(&[]), map(&[("WAREHOUSE_PW", "warehouse_pw")])),
            &job_run(map(&[]), None),
            &task_run_attempt(),
            "/srv/flowlite",
            &BTreeMap::new(),
        ).unwrap_err().to_string();

        assert!(error.contains("WAREHOUSE_PW"), "{error}");
        assert!(error.contains("warehouse_pw"), "{error}");
    }
}
