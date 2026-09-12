//! The tools: arguments in, the same CRUD a CLI command runs, structures out.
//!
//! Each tool mirrors one CLI command's `--json` branch exactly - same filters, same sort,
//! same shape - so an agent reading a run through MCP and a person reading it through the
//! CLI read the same fields. Every call opens its own connection through a freshly named
//! `mem`, never `self.toolkit`'s own: `list_jobs` and `submit_job` seed it, and the other
//! four never read it at all, but taking a fresh name uniformly is one rule instead of
//! two, and it is what lets a file written into `jobs/` after this process started still
//! reach `list_jobs`, and what lets `submit_job` seed the same inline id twice in one
//! session without the second call colliding with the first's rows.
//!
//! `submit_job`, `get_job_run` and `stop_job_run` also take `wait_seconds`, bounded and
//! clamped by `super::wait` - reused rather than copied three times, since the clamp and
//! the "wait nothing can service is refused" rule are each one concept the three calls
//! must not drift apart on.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};

use crate::cli::commands::job::{ensure_data_dir_is_served, installed_job_id};
use crate::cli::commands::job_run::{
    parse_job_run_status, stop_job_run as request_job_run_stop, JobRunDetail, TaskRunAttemptLog,
};
use crate::crud::job::{Job, SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::multistatements::misc::JobIdAlreadyInstalled;
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams;
use crate::crud::CRUD;
use crate::serve_state::{status, ServeStatus};
use crate::toolkit::Toolkit;
use crate::yaml_models::job_yaml::JobYaml;

use super::wait::{clamp_wait_seconds, wait_for_settled_job_run};
use super::McpServer;

/// `get_task_output`'s default, applied per stream when the caller does not name one.
const DEFAULT_MAX_BYTES: usize = 20_000;

/// The label `seed_ad_hoc_job`'s messages read for an inline `yaml` definition, in place of
/// the path a `file` would have - tells an agent where a definition came from without
/// inventing a file that never touched disk.
const INLINE_YAML_LABEL: &str = "<inline yaml>";

#[derive(Debug, Deserialize, JsonSchema)]
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
    /// returns the pending run at once. Clamps to 300 rather than refusing above it; on
    /// elapse the run comes back merely unfinished, never as an error.
    pub wait_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListJobRuns {
    /// Only runs of this job.
    pub job: Option<String>,
    /// Only runs with this status: pending, running, succeeded, failed, skipped, aborted,
    /// timedout or invalid - the same words `job-run list --status` accepts.
    pub status: Option<String>,
    /// How many runs to show, newest first. Defaults to 20.
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetJobRun {
    /// The id of the job run to show.
    pub job_run_id: i64,
    /// Wait up to this many seconds for the run to finish before returning it. Absent or 0
    /// returns it at once. Clamps to 300 rather than refusing above it; on elapse the run
    /// comes back merely unfinished, never as an error.
    pub wait_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StopJobRun {
    /// The id of the job run to stop.
    pub job_run_id: i64,
    /// Wait up to this many seconds for the run to settle before returning it. Absent or 0
    /// returns as soon as the stop has been requested. Clamps to 300 rather than refusing
    /// above it; on elapse the run comes back merely unfinished, never as an error.
    pub wait_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetTaskOutput {
    /// The id of the job run whose task output to read.
    pub job_run_id: i64,
    /// Only this task's attempts, by task id. Every task in the run otherwise.
    pub task: Option<String>,
    /// The most bytes to keep of each stream, counted from the end. Applied to stdout and
    /// stderr independently, on every attempt. Defaults to 20000.
    pub max_bytes: Option<usize>,
}

// `vis = pub(crate)` because `McpServer::new`, in the parent `mcp` module, composes this
// router with `mod.rs`'s own - a private fn (the macro's default) is visible only to this
// module and its descendants, not to the parent that needs to call it.
#[tool_router(router = tools_router, vis = "pub(crate)")]
impl McpServer {

    /// List the jobs declared in the data directory.
    #[tool]
    async fn list_jobs(&self) -> CallToolResult {
        match list_jobs_rows(&self.toolkit).await {
            Ok(jobs) => success_json(jobs),
            Err(err) => error_result(&err),
        }
    }

    /// Submit a run of a job: one installed in the data directory, a file that is never
    /// installed there, or a definition given inline. Mirrors `job submit --json`, with
    /// `wait_seconds` standing in for `--wait` - bounded rather than blocking, since it is
    /// the client's own call timeout, not this server's, that would otherwise cut it off.
    #[tool]
    async fn submit_job(&self, Parameters(args): Parameters<SubmitJob>) -> CallToolResult {
        match submit_job_run(&self.toolkit, args).await {
            Ok((job_run, warning)) => submit_job_result(job_run, warning),
            Err(err) => error_result(&err),
        }
    }

    /// List job runs, newest first.
    #[tool]
    async fn list_job_runs(&self, Parameters(args): Parameters<ListJobRuns>) -> CallToolResult {
        let status = match args.status.as_deref().map(parse_job_run_status) {
            Some(Ok(status)) => Some(status),
            Some(Err(message)) => return error_result(&anyhow::anyhow!(message)),
            None => None,
        };

        let limit = args.limit.unwrap_or(20);

        match list_job_runs_rows(&self.toolkit, args.job, status, limit).await {
            Ok(job_runs) => success_json(job_runs),
            Err(err) => error_result(&err),
        }
    }

    /// Show one job run and the task runs under it. `wait_seconds` waits for it to settle
    /// first, bounded the same way `submit_job`'s is.
    #[tool]
    async fn get_job_run(&self, Parameters(args): Parameters<GetJobRun>) -> CallToolResult {
        match get_job_run_detail(&self.toolkit, args.job_run_id, args.wait_seconds).await {
            Ok(detail) => success_json(detail),
            Err(err) => error_result(&err),
        }
    }

    /// Show what each attempt of a job run wrote to stdout and stderr.
    #[tool]
    async fn get_task_output(&self, Parameters(args): Parameters<GetTaskOutput>) -> CallToolResult {
        let max_bytes = args.max_bytes.unwrap_or(DEFAULT_MAX_BYTES);

        match get_task_output_logs(&self.toolkit, args.job_run_id, args.task.as_deref(), max_bytes).await {
            Ok(logs) => success_json(logs),
            Err(err) => error_result(&err),
        }
    }

    /// Ask for a running job run to be stopped. Mirrors `JobRunStopCmd::run`, but always
    /// returns the `JobRun` row - waited to settle, or merely queued - rather than the
    /// CLI's `{job_run_id, stop_requested}` shape without a wait, so a caller reads
    /// `.status` off the result either way.
    #[tool]
    async fn stop_job_run(&self, Parameters(args): Parameters<StopJobRun>) -> CallToolResult {
        match stop_job_run_and_wait(&self.toolkit, args).await {
            Ok(job_run) => success_json(job_run),
            Err(err) => error_result(&err),
        }
    }

}

/// The tool result for a value whose JSON is already the fact in question: the same
/// pretty-printed text `--json` prints, as the text content every client can read, and the
/// identical value again as `structured_content` for a client that reads results as data
/// rather than text - carried in addition to, never instead of, the text.
///
/// The text is serialized directly from `value`, not from a `serde_json::Value` built from
/// it: `Value`'s map is a `BTreeMap`, so a detour through it would alphabetize field names
/// and stop being byte-identical to what `--json` prints, which serializes the struct
/// directly and so keeps declaration order.
fn success_json(value: impl Serialize) -> CallToolResult {
    let text = serde_json::to_string_pretty(&value)
        .expect("every tool result here is a plain data struct, always representable as JSON");
    let structured = serde_json::to_value(&value)
        .expect("every tool result here is a plain data struct, always representable as JSON");

    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    result.structured_content = Some(structured);
    result
}

/// A tool error carrying the anyhow chain verbatim. `{:#}` joins every `.context()` layer
/// into the one sentence a person at a terminal would read, which is exactly what the
/// model needs to fix its own file - a protocol error would hide this text from it.
fn error_result(err: &anyhow::Error) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(format!("{err:#}"))])
}

/// Keeps at most `max_bytes` of `stream`, from the tail - the end of a log is where the
/// error is. A stream already within the limit passes through byte-identical, which is
/// what keeps an untruncated `get_task_output` stream identical to `job-run logs --json`.
///
/// The cut point is rounded up to the next char boundary, so a multi-byte UTF-8 character
/// is never split - which would otherwise panic when slicing.
fn truncate_tail(stream: &str, max_bytes: usize) -> String {
    if stream.len() <= max_bytes {
        return stream.to_string();
    }

    let cut_at = stream.len() - max_bytes;
    let tail_start = (cut_at..=stream.len())
        .find(|&i| stream.is_char_boundary(i))
        .unwrap_or(stream.len());

    format!("[truncated: {tail_start} earlier bytes dropped]\n{}", &stream[tail_start..])
}

/// `list_jobs`'s own connection: a fresh `mem`, seeded exactly as `job list` seeds its own,
/// so a job file written after this process started is visible without a restart.
async fn list_jobs_rows(toolkit: &Toolkit) -> anyhow::Result<Vec<Job>> {
    let toolkit = toolkit.with_fresh_mem();
    let _memory_conn = toolkit.get_memory_conn().await?;
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));
    crud.init(&mut conn).await?;

    crud.select_jobs(&mut conn, &SelectJobsData {
        filter: SelectJobsDataFilter { job_id: None, name_like: None },
        sort: None,
        limit: None,
        offset: None,
    }).await
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
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)?;
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

/// `submit_job`'s result: the same JSON `job submit --json` prints, as the first content
/// block so a client reading only that one still gets valid JSON, with a second block
/// appended only when nothing is serving the directory this run was just queued against -
/// an agent that got back `pending` with no warning would poll a run that cannot start.
fn submit_job_result(job_run: JobRun, warning: Option<String>) -> CallToolResult {
    let mut result = success_json(job_run);

    if let Some(warning) = warning {
        result.content.push(ContentBlock::text(warning));
    }

    result
}

/// `Some` naming the directory only when nothing at all is serving it - `Starting` counts
/// as served, the same way `ensure_data_dir_is_served` treats it, since that server has the
/// lock and will reach the row this run is queued in. Queuing work for a server that is not
/// up yet is legitimate; an agent that got back `pending` with no warning would poll a run
/// that cannot start until something else changes.
///
/// Never propagates: this runs after `crud.submit_job` has already written the row, so a
/// failed lookup here is a fact about the warning, not about the submit. `status` can fail
/// on an unreadable lock file or state file, and turning that into a tool error would read
/// as "the submit failed" to an agent whose obvious next move is to retry - which would
/// only queue a duplicate of a run that already exists. A lookup failure becomes a warning
/// that says so instead, so the run's id still reaches the caller either way.
fn unserved_directory_warning(data_dir: &str) -> Option<String> {
    match status(Path::new(data_dir)) {
        Ok(ServeStatus::Down) => Some(format!(
            "Nothing is serving {}, so this run will not start until flowlite serve runs \
             against it.",
            data_dir,
        )),
        Ok(ServeStatus::Starting | ServeStatus::Up(_)) => None,
        Err(err) => Some(format!(
            "Could not tell whether {} is being served: {:#}",
            data_dir, err,
        )),
    }
}

/// `list_job_runs`'s own connection. Reads the disk `job_run` table only, but still takes
/// a fresh `mem` name rather than `toolkit`'s own: one rule - every call gets a fresh name
/// - is easier to hold than a rule with an exception for the read-only tools.
async fn list_job_runs_rows(
    toolkit: &Toolkit,
    job: Option<String>,
    status: Option<JobRunStatus>,
    limit: i64,
) -> anyhow::Result<Vec<JobRun>> {
    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    crud.select_job_runs(&mut conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter { id: None, job_id: job, status },
        sort: Some(SelectJobRunsDataSort::IdDesc),
        limit: Some(limit),
        offset: None,
    }).await
}

/// `get_job_run`'s own connection, and the same cross-entity assembly `job-run get` reads
/// through `CRUD::select_job_run_with_task_runs`. A wait, if any, is spent on the run alone
/// - `wait_for_settled_job_run` knows nothing of task runs - and the detail is assembled
/// fresh afterwards either way, so a `wait_seconds` of `0` costs nothing beyond the one
/// query `job-run get` already runs.
async fn get_job_run_detail(
    toolkit: &Toolkit,
    job_run_id: i64,
    wait_seconds: Option<u64>,
) -> anyhow::Result<JobRunDetail> {
    let wait_seconds = clamp_wait_seconds(wait_seconds);

    // Before the wait, for the same reason `job-run stop --wait` checks first: nothing but
    // the serve process settles this row, so waiting on a directory nothing serves is a
    // silent hang.
    if wait_seconds > 0 {
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)?;
    }

    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    if wait_seconds > 0 {
        wait_for_settled_job_run(&crud, &mut conn, job_run_id, wait_seconds).await?;
    }

    let (job_run, task_runs) = crud.select_job_run_with_task_runs(&mut conn, job_run_id).await?;

    Ok(JobRunDetail { job_run, task_runs })
}

/// `get_task_output`'s own connection, and the same cross-entity assembly `job-run logs`
/// reads through `CRUD::select_task_run_attempt_logs` - narrowed by `task_id` exactly as
/// `--task` narrows it, and with each stream truncated to `max_bytes` from the tail.
async fn get_task_output_logs(
    toolkit: &Toolkit,
    job_run_id: i64,
    task_id: Option<&str>,
    max_bytes: usize,
) -> anyhow::Result<Vec<TaskRunAttemptLog>> {
    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    let (task_run_attempts, mut task_run_attempt_output) =
        crud.select_task_run_attempt_logs(&mut conn, job_run_id, task_id).await?;

    Ok(task_run_attempts
        .into_iter()
        .map(|task_run_attempt| {
            let streams = task_run_attempt_output.remove(&task_run_attempt.id).unwrap_or_default();

            truncated_task_run_attempt_log(task_run_attempt, streams, max_bytes)
        })
        .collect())
}

/// One attempt's log, both streams truncated to `max_bytes` independently - pulled out of
/// `get_task_output_logs`'s mapping so a test can build a `TaskRunAttemptLog` the same way
/// the tool does, rather than only exercising `truncate_tail` on strings that were never
/// attached to a stream field.
fn truncated_task_run_attempt_log(
    task_run_attempt: TaskRunAttempt,
    streams: TaskRunAttemptOutputStreams,
    max_bytes: usize,
) -> TaskRunAttemptLog {
    TaskRunAttemptLog {
        task_run_attempt,
        stdout: truncate_tail(&streams.stdout, max_bytes),
        stderr: truncate_tail(&streams.stderr, max_bytes),
    }
}

/// `stop_job_run`'s own connection. Writes the stop row before any wait, mirroring
/// `JobRunStopCmd::run`'s own ordering: the refusal below runs first so a wait nothing can
/// service changes nothing, but once the wait is allowed to proceed the stop is requested
/// regardless of whether `wait_seconds` is `0` - a caller that never asked to wait still
/// gets the stop queued.
async fn stop_job_run_and_wait(toolkit: &Toolkit, args: StopJobRun) -> anyhow::Result<JobRun> {
    let wait_seconds = clamp_wait_seconds(args.wait_seconds);

    if wait_seconds > 0 {
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)?;
    }

    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    request_job_run_stop(&crud, &mut conn, args.job_run_id).await?;

    wait_for_settled_job_run(&crud, &mut conn, args.job_run_id, wait_seconds).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::AppConfig;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;

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

    /// `success_json`'s text must serialize the value directly, not by way of a
    /// `serde_json::Value` (whose map is a `BTreeMap` and would alphabetize field names) -
    /// caught once already, this pins it: a struct declared out of alphabetical order keeps
    /// that order in the text content.
    #[test]
    fn success_json_text_keeps_field_declaration_order_rather_than_alphabetizing() {
        #[derive(Serialize)]
        struct OutOfAlphabeticalOrder {
            zebra: u8,
            apple: u8,
        }

        let result = success_json(OutOfAlphabeticalOrder { zebra: 1, apple: 2 });
        let text = result.content[0].as_text().unwrap().text.as_str();

        assert!(text.find("zebra").unwrap() < text.find("apple").unwrap(), "{text}");
    }

    #[test]
    fn a_stream_under_the_limit_is_returned_byte_identical() {
        assert_eq!(truncate_tail("hello", 20), "hello");
    }

    #[test]
    fn a_stream_at_exactly_the_limit_is_returned_byte_identical() {
        assert_eq!(truncate_tail("hello", 5), "hello");
    }

    #[test]
    fn a_stream_over_the_limit_keeps_the_tail_and_names_the_dropped_count() {
        let truncated = truncate_tail("0123456789", 4);

        assert_eq!(truncated, "[truncated: 6 earlier bytes dropped]\n6789");
    }

    /// The end of a log is where the error is, so the kept bytes are the last ones written,
    /// not the first.
    #[test]
    fn truncation_keeps_the_end_of_the_stream_not_the_start() {
        let truncated = truncate_tail("start-middle-end", 3);

        assert!(truncated.ends_with("end"), "{truncated}");
        assert!(!truncated.contains("start"), "{truncated}");
    }

    /// Splitting on a raw byte offset can land inside a multi-byte character, which would
    /// panic when the tail is sliced out - this keeps the character whole instead.
    #[test]
    fn truncation_never_splits_a_multi_byte_character() {
        let stream = "aé€"; // 1 + 2 + 3 = 6 bytes
        let truncated = truncate_tail(stream, 4);

        assert!(truncated.ends_with('€'), "{truncated}");
    }

    #[test]
    fn an_empty_stream_is_returned_byte_identical() {
        assert_eq!(truncate_tail("", 0), "");
    }

    /// A `TaskRunAttempt` with everything but the id filled with filler - what
    /// `truncated_task_run_attempt_log`'s tests build against, standing in for the row
    /// `select_task_run_attempts` would otherwise have to seed to produce.
    fn task_run_attempt_fixture() -> TaskRunAttempt {
        TaskRunAttempt {
            id: 1,
            task_run_id: 1,
            job_run_id: 1,
            job_id: "job".to_string(),
            task_id: "task".to_string(),
            created_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
            attempt: 1,
            status: TaskRunAttemptStatus::Running,
            process_group_id: None,
        }
    }

    /// The property `stdout_and_stderr_are_truncated_independently` used to claim but not
    /// test: `get_task_output`'s real per-attempt mapping - not `truncate_tail` called
    /// twice on unrelated strings - truncates `stdout` and `stderr` each to `max_bytes`,
    /// on their own budget, and the fields land on the `TaskRunAttemptLog` the tool
    /// actually returns.
    #[test]
    fn stdout_and_stderr_are_truncated_independently() {
        let streams = TaskRunAttemptOutputStreams {
            stdout: "0123456789".to_string(),
            stderr: "short".to_string(),
        };

        let log = truncated_task_run_attempt_log(task_run_attempt_fixture(), streams, 4);

        assert_eq!(log.stdout, "[truncated: 6 earlier bytes dropped]\n6789");
        assert_eq!(log.stderr, "[truncated: 1 earlier bytes dropped]\nhort");
    }

    /// The other half of the same property: a stream within the limit is untouched even
    /// when the other stream on the same attempt is truncated - one stream being cut is
    /// not allowed to affect the other's own byte-identical-when-short guarantee.
    #[test]
    fn a_stream_within_the_limit_is_untouched_while_the_other_is_truncated() {
        let streams = TaskRunAttemptOutputStreams {
            stdout: "0123456789".to_string(),
            stderr: "ok".to_string(),
        };

        let log = truncated_task_run_attempt_log(task_run_attempt_fixture(), streams, 4);

        assert_eq!(log.stdout, "[truncated: 6 earlier bytes dropped]\n6789");
        assert_eq!(log.stderr, "ok");
    }
}
