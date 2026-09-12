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

use crate::cli::commands::job::{ensure_data_dir_is_served, installed_job_id, DataDirNotServed};
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

/// `list_job_runs`'s default page, applied when the caller does not name one.
const DEFAULT_JOB_RUN_LIMIT: i64 = 20;

/// The most runs `list_job_runs` will return in one call. A page this long is already more
/// than an agent reads in one turn; a history of thousands is only a context window spent.
const MAX_JOB_RUN_LIMIT: i64 = 200;

/// The label `seed_ad_hoc_job`'s messages read for an inline `yaml` definition, in place of
/// the path a `file` would have - tells an agent where a definition came from without
/// inventing a file that never touched disk.
const INLINE_YAML_LABEL: &str = "<inline yaml>";

// `deny_unknown_fields` on all five argument structs: a key the struct does not declare is
// a typo or a guess, and serde's default is to ignore it silently. On this struct that
// meant a misspelled `params` submitting the job with its default parameters instead of the
// agent's - a wrong result reported as a success, on the one tool that writes. rmcp adds
// nothing of its own to an arguments object (it deserializes the caller's `arguments` map
// verbatim: `Parameters`' `FromContextPart` impl), so every key here really is the caller's.
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListJobRuns {
    /// Only runs of this job.
    pub job: Option<String>,
    /// Only runs with this status: pending, running, succeeded, failed, skipped, aborted,
    /// timedout or invalid.
    pub status: Option<String>,
    /// How many runs to show, newest first. Defaults to 20. A value above 200 shows 200,
    /// and one below 1 shows the default.
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetJobRun {
    /// The id of the job run to show.
    pub job_run_id: i64,
    /// Wait up to this many seconds for the run to finish before returning it. Absent or 0
    /// returns it at once. A value above 300 waits 300. If the wait runs out the run comes
    /// back unfinished rather than as an error.
    pub wait_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StopJobRun {
    /// The id of the job run to stop.
    pub job_run_id: i64,
    /// Wait up to this many seconds for the run to settle before returning it. Absent or 0
    /// returns as soon as the stop has been requested. A value above 300 waits 300. If the
    /// wait runs out the run comes back unfinished rather than as an error.
    pub wait_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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

    /// List job runs, newest first.
    #[tool]
    async fn list_job_runs(&self, Parameters(args): Parameters<ListJobRuns>) -> CallToolResult {
        let status = match args.status.as_deref().map(parse_job_run_status) {
            Some(Ok(status)) => Some(status),
            Some(Err(message)) => return error_result(&anyhow::anyhow!(message)),
            None => None,
        };

        let limit = clamp_job_run_limit(args.limit);

        match list_job_runs_rows(&self.toolkit, args.job, status, limit).await {
            Ok(job_runs) => success_json(job_runs),
            Err(err) => error_result(&err),
        }
    }

    /// Show one job run and the task runs under it.
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

    // Mirrors `JobRunStopCmd::run`, but always returns the `JobRun` row rather than the
    // CLI's `{job_run_id, stop_requested}` shape without a wait, so a caller reads
    // `.status` off the result either way. Kept out of the `///` above for the reason
    // `submit_job`'s rationale is: the model pays for that text on every turn.
    /// Ask for a running job run to be stopped. Returns the job run, whose status is
    /// settled only if `wait_seconds` was long enough; without one it is merely the run as
    /// it stands, with the stop requested.
    #[tool]
    async fn stop_job_run(&self, Parameters(args): Parameters<StopJobRun>) -> CallToolResult {
        match stop_job_run_and_wait(&self.toolkit, args).await {
            Ok((job_run, warning)) => job_run_result(job_run, warning),
            Err(err) => error_result(&err),
        }
    }

}

/// The tool result for a value whose JSON is already the fact in question: the same
/// pretty-printed text `--json` prints, as the text content every client can read, and the
/// identical value again as `structured_content` for a client that reads results as data
/// rather than text - carried in addition to, never instead of, the text.
///
/// `structured_content` is therefore exactly what content[0] says and nothing else, which
/// is why `job_run_result` clears it rather than adding a warning beside the value: see
/// there.
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

/// Turns `ensure_data_dir_is_served`'s typed refusal into these tools' own words, the way
/// `describe_ad_hoc_job_collision` does for a collision: nothing here was a flag to drop, so
/// the remedy names `wait_seconds`, the argument the caller actually sent. The CLI's wording
/// of the same fact - "drop --wait" - would send an agent looking for a flag no tool takes,
/// whose only repairs are to retry unchanged or to invent one. Any other error passes
/// through unchanged.
fn describe_unserved_data_dir(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<DataDirNotServed>() {
        Ok(unserved) => anyhow::anyhow!(
            "{}, so waiting would only run the wait_seconds out - nothing is there to move \
             the run along. Start flowlite serve against it, or call again without \
             wait_seconds to get the run back as it stands.",
            unserved,
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

/// The result `submit_job` and `stop_job_run` both return: the same JSON `--json` prints,
/// as the first content block so a client reading only that one still gets valid JSON, with
/// a second block appended only when nothing is serving the directory the run is in - an
/// agent that got back `pending` with no warning would poll a status that cannot change.
///
/// A warned result carries no `structured_content` at all. The alternative was to add the
/// warning beside the run in the structured value, and that is worse: a client that reads
/// `structured_content` and ignores the text would otherwise be handed `{"status":
/// "pending"}` with nothing saying it will never move, which is the exact failure the
/// warning exists to prevent, and giving that field one shape when warned and another when
/// not is a trap of its own. Dropping it leaves such a client with the text blocks, which
/// carry both facts - the shape every MCP client is required to read.
fn job_run_result(job_run: JobRun, warning: Option<String>) -> CallToolResult {
    let mut result = success_json(job_run);

    if let Some(warning) = warning {
        result.content.push(ContentBlock::text(warning));
        result.structured_content = None;
    }

    result
}

/// `Some` naming the directory only when nothing at all is serving it - `Starting` counts
/// as served, the same way `ensure_data_dir_is_served` treats it, since that server has the
/// lock and will reach the row. Writing to a directory whose server is not up yet is
/// legitimate; an agent that got back `pending` with no warning would poll a status that
/// cannot change until something else does.
///
/// One sentence for both writing tools: the serve process is what would start the run
/// `submit_job` queued and what would act on the stop row `stop_job_run` wrote, so "the
/// status will not change" is the one fact either caller needs, and neither has a remedy
/// the other does not.
///
/// Never propagates: both callers read it after their own row is already written, so a
/// failed lookup here is a fact about the warning, not about the write. `status` can fail
/// on an unreadable lock file or state file, and turning that into a tool error would read
/// as "the write failed" to an agent whose obvious next move is to retry - which for a
/// submit would only queue a duplicate of a run that already exists. A lookup failure
/// becomes a warning that says so instead, so the run still reaches the caller either way.
fn unserved_directory_warning(data_dir: &str) -> Option<String> {
    match status(Path::new(data_dir)) {
        Ok(ServeStatus::Down) => Some(format!(
            "Nothing is serving {}, so this run's status will not change until flowlite \
             serve runs against it.",
            data_dir,
        )),
        Ok(ServeStatus::Starting | ServeStatus::Up(_)) => None,
        Err(err) => Some(format!(
            "Could not tell whether {} is being served: {:#}",
            data_dir, err,
        )),
    }
}

/// `list_job_runs`'s bound, the counterpart of `clamp_wait_seconds`: of the three tools
/// that can flood a context window, this was the one left unbounded.
///
/// Anything below 1 means the default rather than "everything". `limit` is bound straight
/// into SQL `LIMIT ?`, and SQLite reads a negative LIMIT as no limit at all - so `-1` asked
/// through this tool returned the whole run history, the opposite of what a smaller number
/// asks for.
fn clamp_job_run_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(limit) if limit > 0 => limit.min(MAX_JOB_RUN_LIMIT),
        _ => DEFAULT_JOB_RUN_LIMIT,
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
/// through `CRUD::select_job_run_with_task_runs`. A wait, if any, is spent on the run
/// alone (`wait_for_settled_job_run` knows nothing of task runs) and the detail is
/// assembled fresh afterwards either way, so a `wait_seconds` of `0` costs nothing beyond
/// the one query `job-run get` already runs.
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
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)
            .map_err(describe_unserved_data_dir)?;
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
///
/// Carries the same warning `submit_job_run` does, for the same reason: without a wait the
/// run comes back `pending` or `running`, and against an unserved directory that status will
/// never change, because only the serve process reads the stop row. `.status` is what this
/// tool's own contract points a caller at, so the silence was a misleading answer rather
/// than merely a missing one.
async fn stop_job_run_and_wait(
    toolkit: &Toolkit,
    args: StopJobRun,
) -> anyhow::Result<(JobRun, Option<String>)> {

    let wait_seconds = clamp_wait_seconds(args.wait_seconds);

    if wait_seconds > 0 {
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)
            .map_err(describe_unserved_data_dir)?;
    }

    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    request_job_run_stop(&crud, &mut conn, args.job_run_id).await?;

    let job_run = wait_for_settled_job_run(&crud, &mut conn, args.job_run_id, wait_seconds).await?;

    // After the stop row is written, for the reason `submit_job_run` reads it after its own
    // write: by this point the stop is queued, and a `status` read failure here is a fact
    // about the warning, not about the stop.
    let warning = unserved_directory_warning(&crud.toolkit.app_config.data_dir);

    Ok((job_run, warning))
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

    /// The same rule on a read tool, so the property is pinned as one that holds for every
    /// argument struct rather than only for the one that writes.
    #[test]
    fn a_misspelled_argument_key_on_a_read_tool_is_refused_too() {
        let error = serde_json::from_value::<ListJobRuns>(serde_json::json!({
            "limmit": 5,
        })).unwrap_err().to_string();

        assert!(error.contains("limmit"), "{error}");
    }

    /// The bug this clamp exists for: `limit` is bound into SQL `LIMIT ?`, and SQLite reads
    /// a negative LIMIT as no limit at all, so `-5` returned the entire run history.
    #[test]
    fn a_negative_limit_means_the_default_not_everything() {
        assert_eq!(clamp_job_run_limit(Some(-5)), DEFAULT_JOB_RUN_LIMIT);
        assert_eq!(clamp_job_run_limit(Some(-1)), DEFAULT_JOB_RUN_LIMIT);
    }

    /// `0` is the other non-positive case, and SQLite would honour it literally - an empty
    /// page is not what a caller asking for "no limit in particular" means either.
    #[test]
    fn a_zero_limit_means_the_default() {
        assert_eq!(clamp_job_run_limit(Some(0)), DEFAULT_JOB_RUN_LIMIT);
    }

    #[test]
    fn an_absent_limit_means_the_default() {
        assert_eq!(clamp_job_run_limit(None), DEFAULT_JOB_RUN_LIMIT);
    }

    /// Clamped rather than refused, the same way `clamp_wait_seconds` clamps: a page of 200
    /// is still an answer in the shape the caller already handles.
    #[test]
    fn a_limit_above_the_cap_clamps_to_it() {
        assert_eq!(clamp_job_run_limit(Some(1_000_000)), MAX_JOB_RUN_LIMIT);
        assert_eq!(clamp_job_run_limit(Some(MAX_JOB_RUN_LIMIT + 1)), MAX_JOB_RUN_LIMIT);
    }

    #[test]
    fn a_limit_at_or_below_the_cap_is_used_as_given() {
        assert_eq!(clamp_job_run_limit(Some(MAX_JOB_RUN_LIMIT)), MAX_JOB_RUN_LIMIT);
        assert_eq!(clamp_job_run_limit(Some(1)), 1);
    }

    /// The remedy an agent reads names the argument it actually sent. The CLI's own wording
    /// of this same typed fact says "drop --wait", which is a flag no tool here takes.
    #[test]
    fn the_tool_wording_of_an_unserved_data_dir_names_wait_seconds_not_a_flag() {
        let unserved = DataDirNotServed { data_dir: "./d1".to_string() };
        let error = describe_unserved_data_dir(unserved.into()).to_string();

        assert!(error.contains("./d1"), "{error}");
        assert!(error.contains("wait_seconds"), "{error}");
        assert!(!error.contains("--wait"), "{error}");
    }

    /// One typed fact reworded, not a catch-all - a `status` read failure inside the check
    /// must not be relabelled as an unserved directory.
    #[test]
    fn the_tool_wording_leaves_any_other_error_alone() {
        let error = describe_unserved_data_dir(anyhow::anyhow!("Job run 7 not found"));

        assert_eq!(error.to_string(), "Job run 7 not found");
    }

    /// A warned result drops `structured_content` rather than carrying a run whose status
    /// the warning contradicts - a client that reads only the structured value would
    /// otherwise be handed `pending` with nothing saying it will never move.
    #[test]
    fn a_warned_result_carries_no_structured_content_and_still_leads_with_json() {
        let result = job_run_result(job_run_fixture(), Some("nothing is serving it".to_string()));

        assert!(result.structured_content.is_none(), "{:?}", result.structured_content);
        assert_eq!(result.content.len(), 2);

        let text = result.content[0].as_text().unwrap().text.as_str();
        let parsed: serde_json::Value = serde_json::from_str(text).expect(text);
        assert_eq!(parsed["status"], serde_json::json!("pending"));

        assert_eq!(result.content[1].as_text().unwrap().text, "nothing is serving it");
    }

    /// The unwarned half of the same rule: nothing changes for the ordinary result, which
    /// still carries the run as structured content beside the identical text.
    #[test]
    fn an_unwarned_result_still_carries_the_run_as_structured_content() {
        let result = job_run_result(job_run_fixture(), None);

        assert_eq!(result.content.len(), 1);
        assert_eq!(result.structured_content.unwrap()["status"], serde_json::json!("pending"));
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

    /// A pending `JobRun` with everything but its status filled with filler - what
    /// `job_run_result`'s tests build against, standing in for a row `submit_job` would
    /// otherwise have to seed a whole data directory to produce.
    fn job_run_fixture() -> JobRun {
        JobRun {
            id: 1,
            job_id: "job".to_string(),
            job_name: "Job".to_string(),
            job_description: String::new(),
            parameters: sqlx::types::Json(BTreeMap::new()),
            created_at: chrono::Utc::now(),
            scheduled_at: None,
            started_at: None,
            finished_at: None,
            status: JobRunStatus::Pending,
        }
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
