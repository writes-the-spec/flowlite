//! `submit_job`: the one tool that writes a run, from a job installed in the data
//! directory, a file that is never installed there, or a definition given inline.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::crud::job_run::JobRun;
use crate::crud::multistatements::misc::JobIdAlreadyInstalled;
use crate::crud::CRUD;
use crate::mcp::wait::{clamp_wait_seconds, wait_for_settled_job_run};
use crate::mcp::McpServer;
use crate::shared::job::installed_job_id;
use crate::shared::wait::ensure_data_dir_is_served;
use crate::toolkit::Toolkit;
use crate::yaml_models::job_yaml::JobYaml;

use super::result::{describe_unserved_data_dir, error_result, job_run_result, unserved_directory_warning};

/// The label this tool's messages read for an inline `yaml` definition, in place of the
/// path a `file` would have - tells an agent where a definition came from without
/// inventing a file that never touched disk.
const INLINE_YAML_LABEL: &str = "<inline yaml>";

// `deny_unknown_fields` is the rule on every argument struct (see the module doc), and this
// is the struct it was written for: a misspelled `params` submitted the job with its
// default parameters instead of the agent's - a wrong result reported as a success, on the
// one tool that writes.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubmitJob {
    /// The id of a job installed in the data directory. Exactly one of job, file or yaml
    /// is required.
    pub job: Option<String>,
    /// A path to a job definition that is not installed there, read where it lies rather
    /// than copied in. Exactly one of job, file or yaml is required.
    pub file: Option<String>,
    /// A job definition, inline - nothing is written to disk. Exactly one of job, file or
    /// yaml is required.
    pub yaml: Option<String>,
    /// Values for the job's declared parameters, by name. A name the job does not declare
    /// is refused.
    pub params: Option<BTreeMap<String, String>>,
    /// Wait up to this many seconds for the run to finish before returning it. Absent or 0
    /// returns the pending run at once. A value above 300 waits 300. If the wait runs out
    /// the run comes back unfinished rather than as an error.
    pub wait_seconds: Option<u64>,
}

#[tool_router(router = submit_job_router, vis = "pub(super)")]
impl McpServer {

    // Mirrors `job submit --json`, with `wait_seconds` standing in for `--wait` - bounded
    // rather than blocking, since it is the client's own call timeout, not this server's,
    // that would otherwise cut a longer wait off. Kept out of the `///` above: every word
    // there is sent to the model on every turn, and none of this helps it choose the tool
    // or fill an argument.
    /// Submit a run of a job: one installed in the data directory, a file that is never
    /// installed there, or a definition given inline. Returns the job run, still pending
    /// unless `wait_seconds` was long enough for it to finish.
    #[tool]
    async fn submit_job(&self, Parameters(args): Parameters<SubmitJob>) -> CallToolResult {
        match submit_job_run(&self.toolkit, args).await {
            Ok((job_run, warning)) => job_run_result(job_run, warning),
            Err(err) => error_result(&err),
        }
    }
}

/// Which of the three ways to name a job's definition the caller gave, resolved once so
/// the exactly-one-of-three rule and the branch that acts on it cannot drift apart.
#[derive(Debug)]
enum SubmitJobDefinition {
    Job(String),
    File(String),
    Yaml(String),
}

/// The hand-written counterpart of the `clap::ArgGroup` on `JobSubmitCmd`: rmcp's derive
/// has no equivalent for "exactly one of these fields", so it is checked here instead -
/// which is exactly why this needs its own unit test.
fn resolve_definition(args: &SubmitJob) -> anyhow::Result<SubmitJobDefinition> {
    let given: Vec<&str> = [
        args.job.is_some().then_some("job"),
        args.file.is_some().then_some("file"),
        args.yaml.is_some().then_some("yaml"),
    ].into_iter().flatten().collect();

    match given.as_slice() {
        ["job"] => Ok(SubmitJobDefinition::Job(args.job.clone().unwrap())),
        ["file"] => Ok(SubmitJobDefinition::File(args.file.clone().unwrap())),
        ["yaml"] => Ok(SubmitJobDefinition::Yaml(args.yaml.clone().unwrap())),
        [] => anyhow::bail!(
            "Name exactly one of job, file or yaml to submit a job; none was given."
        ),
        _ => anyhow::bail!(
            "Name exactly one of job, file or yaml to submit a job; got {}.",
            given.join(" and "),
        ),
    }
}

/// Turns `seed_ad_hoc_job`'s typed collision into this tool's own words for "submit it by
/// name instead": unlike the CLI's `-f`, nothing here was a flag to drop, so the remedy is
/// the shape this tool itself takes for an installed job. Any other error passes through
/// unchanged.
fn describe_ad_hoc_job_collision(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<JobIdAlreadyInstalled>() {
        Ok(collision) => anyhow::anyhow!(
            r#"{}. Submit it by name instead: {{ "job": "{}" }}"#,
            collision,
            collision.job_id,
        ),
        Err(err) => err,
    }
}

/// `submit_job`'s own connection: a fresh `mem`, seeded exactly as `job submit` seeds its
/// own, so the same inline id can be submitted twice in one session without the second
/// call colliding with the first's rows, and a job file just written into `jobs/` is
/// visible without a restart.
async fn submit_job_run(toolkit: &Toolkit, args: SubmitJob) -> anyhow::Result<(JobRun, Option<String>)> {
    let definition = resolve_definition(&args)?;
    let overrides = args.params.unwrap_or_default();
    let wait_seconds = clamp_wait_seconds(args.wait_seconds);

    // Before the run is written, so a wait that cannot be serviced leaves no queued run
    // behind for a server that is not there to run it - the same ordering `job submit
    // --wait` keeps.
    if wait_seconds > 0 {
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)
            .map_err(describe_unserved_data_dir)?;
    }

    let toolkit = toolkit.with_fresh_mem();
    let _memory_conn = toolkit.get_memory_conn().await?;
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));
    let row_id = crud.init(&mut conn).await?;

    let job_id = match definition {
        SubmitJobDefinition::Job(job_name) => installed_job_id(&crud, &mut conn, &job_name).await?,

        SubmitJobDefinition::File(file) => {
            let path = PathBuf::from(file);
            let job_yaml = JobYaml::from_yaml(&path)?;

            crud.seed_ad_hoc_job(&mut conn, job_yaml, &path, row_id).await
                .map_err(describe_ad_hoc_job_collision)?
        }

        SubmitJobDefinition::Yaml(yaml) => {
            let job_yaml = JobYaml::from_yaml_str(&yaml, INLINE_YAML_LABEL)?;

            crud.seed_ad_hoc_job(&mut conn, job_yaml, Path::new(INLINE_YAML_LABEL), row_id).await
                .map_err(describe_ad_hoc_job_collision)?
        }
    };

    let job_run_id = crud.submit_job(&mut conn, &job_id, &overrides, None).await?;
    let job_run = wait_for_settled_job_run(&crud, &mut conn, job_run_id, wait_seconds).await?;

    // A lookup failure here is not a failure to submit - the run is already written by
    // this point, so surfacing it as a tool error would read as "the submit failed" to an
    // agent whose obvious next move is to retry, queuing a duplicate run for one that
    // already exists. Degrading to a warning instead keeps the run's id in the agent's
    // hands either way.
    let warning = unserved_directory_warning(&crud.toolkit.app_config.data_dir);

    Ok((job_run, warning))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::AppConfig;

    /// The regression test for the whole fresh-mem mechanism this cut exists for: two
    /// `submit_job_run` calls seeding the same inline id, joined so they genuinely run
    /// concurrently over one `Toolkit` - not one after the other, which would prove nothing:
    /// `mem` is a shared-cache database that SQLite drops the instant nothing has it open
    /// (`src/toolkit.rs`), so a sequential first call's rows are already gone by the time a
    /// later, second call starts, and there would be nothing left to collide with.
    ///
    /// Relies on `src/toolkit.rs`'s `MIGRATION_LOCK` to keep the two calls' own disk-schema
    /// migrations (each call's `get_conn` runs one) from racing each other on the file both
    /// calls share - a real hazard, but a separate one from the `mem` collision this test
    /// exists to catch, and not one either call here is supposed to be exercising.
    #[tokio::test]
    async fn two_joined_submits_of_the_same_inline_id_do_not_collide() {
        let data_dir = std::env::temp_dir().join(format!("flowlite-mcp-concurrent-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(data_dir.join("jobs")).unwrap();

        let toolkit = Toolkit::new(AppConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..AppConfig::default()
        });

        let yaml = "id: probe\nname: Probe\ntasks:\n  - id: say\n    command: \"true\"\n".to_string();
        let args = || SubmitJob { job: None, file: None, yaml: Some(yaml.clone()), params: None, wait_seconds: None };

        let (first, second) = tokio::join!(
            submit_job_run(&toolkit, args()),
            submit_job_run(&toolkit, args()),
        );

        let (first_run, _) = first.unwrap();
        let (second_run, _) = second.unwrap();

        assert_eq!(first_run.job_id, "probe");
        assert_eq!(second_run.job_id, "probe");
        assert_ne!(first_run.id, second_run.id, "two submits must get two different run ids");

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    fn submit_job_args(job: Option<&str>, file: Option<&str>, yaml: Option<&str>) -> SubmitJob {
        SubmitJob {
            job: job.map(str::to_string),
            file: file.map(str::to_string),
            yaml: yaml.map(str::to_string),
            params: None,
            wait_seconds: None,
        }
    }

    /// The hand-written counterpart of clap's `ArgGroup` - none of the three is a tool
    /// error naming that none was given.
    #[test]
    fn resolving_a_definition_with_none_of_the_three_given_is_refused() {
        let error = resolve_definition(&submit_job_args(None, None, None)).unwrap_err().to_string();

        assert!(error.contains("none was given"), "{error}");
    }

    /// Two together is refused too, and the message says which two - this is the case a
    /// clap `ArgGroup` would catch for free, and precisely why this needs its own test.
    ///
    /// Asserted on the tail (`ends_with`) rather than `contains("job")`/`contains("file")`:
    /// the static prefix "Name exactly one of job, file or yaml..." already contains all
    /// three field names, so a `contains` check here would pass no matter which pair was
    /// actually given - or even if the "got ..." tail naming them were dropped entirely.
    #[test]
    fn resolving_a_definition_with_job_and_file_given_names_both_in_the_refusal() {
        let error = resolve_definition(&submit_job_args(Some("etl"), Some("f.yaml"), None))
            .unwrap_err()
            .to_string();

        assert!(error.ends_with("got job and file."), "{error}");
    }

    /// The other two pairs, each checked against its own tail so a bug that always reports
    /// "job and file" regardless of what was actually given would be caught here.
    #[test]
    fn resolving_a_definition_with_job_and_yaml_given_names_both_in_the_refusal() {
        let error = resolve_definition(&submit_job_args(Some("etl"), None, Some("id: x")))
            .unwrap_err()
            .to_string();

        assert!(error.ends_with("got job and yaml."), "{error}");
    }

    #[test]
    fn resolving_a_definition_with_file_and_yaml_given_names_both_in_the_refusal() {
        let error = resolve_definition(&submit_job_args(None, Some("f.yaml"), Some("id: x")))
            .unwrap_err()
            .to_string();

        assert!(error.ends_with("got file and yaml."), "{error}");
    }

    /// All three at once is refused the same way as any other pair, naming all three.
    #[test]
    fn resolving_a_definition_with_all_three_given_names_all_three_in_the_refusal() {
        let error = resolve_definition(&submit_job_args(Some("etl"), Some("f.yaml"), Some("id: x")))
            .unwrap_err()
            .to_string();

        assert!(error.ends_with("got job and file and yaml."), "{error}");
    }

    #[test]
    fn resolving_a_definition_with_only_job_given_is_accepted() {
        let definition = resolve_definition(&submit_job_args(Some("etl"), None, None)).unwrap();

        assert!(matches!(definition, SubmitJobDefinition::Job(job) if job == "etl"));
    }

    #[test]
    fn resolving_a_definition_with_only_file_given_is_accepted() {
        let definition = resolve_definition(&submit_job_args(None, Some("f.yaml"), None)).unwrap();

        assert!(matches!(definition, SubmitJobDefinition::File(file) if file == "f.yaml"));
    }

    #[test]
    fn resolving_a_definition_with_only_yaml_given_is_accepted() {
        let definition = resolve_definition(&submit_job_args(None, None, Some("id: x"))).unwrap();

        assert!(matches!(definition, SubmitJobDefinition::Yaml(yaml) if yaml == "id: x"));
    }

    /// `params` is a JSON object on the wire; this pins that it lands as the same
    /// `BTreeMap<String, String>` `--param` builds, rather than a `serde_json::Value` the
    /// rest of `submit_job_run` would have to convert.
    #[test]
    fn a_params_object_deserializes_into_the_map_param_builds() {
        let args: SubmitJob = serde_json::from_value(serde_json::json!({
            "job": "etl",
            "params": { "region": "eu", "date": "2026-09-11" },
        })).unwrap();

        let params = args.params.unwrap();
        assert_eq!(params.get("region").map(String::as_str), Some("eu"));
        assert_eq!(params.get("date").map(String::as_str), Some("2026-09-11"));
    }

    /// A key the struct does not declare is refused rather than ignored: serde's default
    /// would have run this job with its declared defaults instead of the parameters the
    /// agent actually sent, and reported that as a success.
    #[test]
    fn a_misspelled_argument_key_is_refused_rather_than_ignored() {
        let error = serde_json::from_value::<SubmitJob>(serde_json::json!({
            "job": "etl",
            "parmas": { "region": "eu" },
        })).unwrap_err().to_string();

        assert!(error.contains("parmas"), "{error}");
    }

    /// `submit_job` without `job`, `file` or `yaml` at all deserializes fine - `params`
    /// alone would otherwise look like a fourth way in.
    #[test]
    fn a_params_object_with_no_definition_still_deserializes() {
        let args: SubmitJob = serde_json::from_value(serde_json::json!({
            "params": { "region": "eu" },
        })).unwrap();

        assert!(args.job.is_none());
        assert!(args.file.is_none());
        assert!(args.yaml.is_none());
    }
}
