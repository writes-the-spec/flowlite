//! What `flowlite init` lays into a data directory, and the files themselves.
//!
//! Here rather than in `src/cli/` because the MCP `init_data_dir` tool scaffolds the same
//! directory the command does, and a frontend may not import another's. What stays
//! frontend-side is the wording: the command's report is prose for a terminal, the tool's
//! result is JSON for a model, and neither is the fact the other needs.

use std::path::Path;
use anyhow::Context;
use serde::Serialize;


/// One file `init` was asked to lay down, and whether it had to.
#[derive(Serialize)]
pub struct ScaffoldedFile {
    /// Serialized as `path`: a caller reads where the file landed, not what this field is
    /// called here.
    #[serde(rename = "path")]
    pub relative_path: &'static str,
    pub created: bool,
}


/// What one `init` did, as an agent reads it. The command has no `--json` of its own - it
/// prints prose - so this shape is the MCP tool's, and the directory is named in it because
/// the tool takes no argument saying which one it acted on.
#[derive(Serialize)]
pub struct InitResult {
    pub data_dir: String,
    pub files: Vec<ScaffoldedFile>,
}


/// What a fresh data directory gets, in the order the report prints it. The job and the
/// schedule are one example between them - the schedule fires the job id the job file
/// declares - so renaming either without the other is a startup error, which
/// `the_example_schedule_parses_and_fires_the_job_beside_it` is there to catch.
const SCAFFOLD: [(&str, &str); 3] = [
    ("jobs/hello.yaml", JOB_YAML),
    ("schedules/daily-hello.yaml", SCHEDULE_YAML),
    ("config.toml", CONFIG_TOML),
];


const JOB_YAML: &str = r#"id: hello-world
name: Hello World
description: The example job `flowlite init` leaves behind. Edit it, or delete it once you have your own.
tasks:
  - id: say-hello
    description: Says hello
    command: echo "hello from flowlite"

  # Tasks run in parallel unless depends_on puts them in order. This one waits for
  # say-hello, and is skipped if say-hello does not succeed.
  - id: say-goodbye
    description: Says goodbye, once say-hello has succeeded
    depends_on: [say-hello]
    command: echo "goodbye from flowlite"
"#;


const SCHEDULE_YAML: &str = r#"id: daily-hello
name: Daily hello
description: Submits the example job once a day.
# Six fields, seconds first: sec min hour day-of-month month day-of-week.
# A pasted five-field crontab line is rejected. This is 09:00:00 every day.
cron: "0 0 9 * * *"
# The zone the cron fields are read in, so the run does not drift with DST.
timezone: UTC
# true keeps the file and stops the firing.
disabled: false
jobs:
  - id: hello-world
"#;


/// Every line is a comment, so a directory `init` made behaves exactly as one with no
/// config.toml at all. It is a map of what can be set and what each key defaults to -
/// uncommenting a line at its default would write that default down a second time, and the
/// copy on disk is the one that goes stale when the code's default moves.
const CONFIG_TOML: &str = r#"# flowlite configuration. Everything here has a default, so this file can stay commented
# out entirely. Uncomment only what you want to change; a file naming a single key leaves
# every other default alone. Each key can also be set as an environment variable:
# FLOWLITE_UI__PAGE_SIZE=10, FLOWLITE_ORCHESTRATOR__POLL_INTERVAL_SECONDS=5.
#
# The data directory itself is the one thing not worth setting here - naming it in a file
# inside it is circular. Pass -D / --data-dir / FLOWLITE_DATA_DIR.

# [orchestrator]
# poll_interval_seconds = 1       # how often a service looks for work itself
# error_backoff_seconds = 5       # pause before a failed service restarts
# reader_eof_timeout_seconds = 2  # wait for a finished attempt's output to end
# max_stream_bytes = 1048576      # per stream, per attempt, then truncated
# read_buffer_bytes = 8192        # one read from a running command's pipe
# max_running_attempts = 32       # running task attempts across every job, 0 for no limit

# [ui]
# page_size = 25                  # rows per page on the run, job and schedule lists
# max_page_size = 100             # the largest ?page_size= the run list accepts
# refresh_interval_seconds = 3    # how often a page showing a live run refreshes

# What a job's YAML gets for a field it leaves out, read when that YAML is read - at
# startup. A run already submitted keeps the values it was submitted with.
# [job_defaults]
# timeout_seconds = 3600          # what a task with no timeout: gets
# max_retries = 0
# retry_delay_seconds = 60
# max_parallel_runs = 1           # what a job with no max_parallel_runs: gets

# [schedule_defaults]
# timezone = "UTC"                # what a schedule with no timezone: reads its cron in

# Where mail notifications are sent from. No defaults: leave the section out and the email
# channel is off entirely. The password belongs in the environment on a real box -
# FLOWLITE_SMTP__PASSWORD - rather than in this file.
# [smtp]
# host = "smtp.example.com"       # required
# from = "flowlite@example.com"   # required
# port = 587
# username = ""                   # empty for a relay that authenticates nobody
# encryption = "starttls"         # "starttls", "tls" or "none"
# max_output_bytes = 4096         # per stream, per failed task, in the message

# The workspace Slack notifications are posted to. No defaults, for the reason [smtp] has
# none. The token belongs in the environment on a real box - FLOWLITE_SLACK__TOKEN.
# [slack]
# token = "xoxb-..."

# What a job's secret_env: resolves its values against, by name. A name is [a-z0-9_]+ with
# no __ in it, so either spelling reaches the same name. The file suits a development box;
# FLOWLITE_SECRETS__WAREHOUSE_PW=hunter2 suits a real one, since a value written here sits
# in the data directory beside the jobs/ you were told to commit.
# [secrets]
# warehouse_pw = "hunter2"

# What a job's limits: resolves the name it claims to a maximum. Read the same two ways as
# [secrets] - FLOWLITE_CONCURRENCY_LIMITS__WAREHOUSE=3 - but there is nothing to hide in a
# limit. The name `global` is reserved and refused at startup: `flowlite limits` prints the
# combined cap across every job under it.
# [concurrency_limits]
# warehouse = 3                   # 0 for no limit
"#;


/// Lays the example files down, leaving any that are already there untouched: re-running
/// `init` in a directory someone has been working in must not be how they lose that work.
pub fn scaffold(data_dir: &Path) -> anyhow::Result<Vec<ScaffoldedFile>> {

    for directory in ["jobs", "schedules"] {
        let path = data_dir.join(directory);
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Failed to create {}", path.display()))?;
    }

    let mut files = Vec::new();

    for (relative_path, contents) in SCAFFOLD {

        let path = data_dir.join(relative_path);
        let created = !path.exists();

        if created {
            std::fs::write(&path, contents)
                .with_context(|| format!("Failed to write {}", path.display()))?;
        }

        files.push(ScaffoldedFile { relative_path, created });
    }

    Ok(files)
}


#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use super::*;
    use crate::yaml_models::job_yaml::JobYaml;
    use crate::yaml_models::schedule_yaml::ScheduleYaml;

    fn a_temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("flowlite-init-test-{}", uuid::Uuid::new_v4()))
    }

    /// The directory named by `-D` need not exist yet, which is the whole point of the
    /// command: `flowlite -D new-project init` is the first thing anyone runs.
    #[test]
    fn a_directory_that_does_not_exist_yet_is_created_with_its_three_files() {
        let data_dir = a_temp_dir();

        scaffold(&data_dir).unwrap();

        assert!(data_dir.join("jobs/hello.yaml").exists());
        assert!(data_dir.join("schedules/daily-hello.yaml").exists());
        assert!(data_dir.join("config.toml").exists());
    }

    /// A scaffold that does not parse would fail at the first `flowlite serve`, which is
    /// the one moment a new user has no way to tell their mistake from ours.
    #[test]
    fn the_example_job_parses_as_a_job() {
        let data_dir = a_temp_dir();

        scaffold(&data_dir).unwrap();

        let job = JobYaml::from_yaml(&data_dir.join("jobs/hello.yaml")).unwrap();

        assert_eq!(job.id, "hello-world");
        assert_eq!(job.tasks.len(), 2);
    }

    /// The two files are only an example of anything together: a schedule naming a job id
    /// no file declares is a startup error, and a renamed example is exactly how that
    /// would happen.
    #[test]
    fn the_example_schedule_parses_and_fires_the_job_beside_it() {
        let data_dir = a_temp_dir();

        scaffold(&data_dir).unwrap();

        let job = JobYaml::from_yaml(&data_dir.join("jobs/hello.yaml")).unwrap();
        let schedule = ScheduleYaml::from_yaml(&data_dir.join("schedules/daily-hello.yaml")).unwrap();

        assert_eq!(schedule.jobs.len(), 1);
        assert_eq!(schedule.jobs[0].id, job.id);
    }

    /// Re-running `init` in a directory someone has been working in must not be the way
    /// they lose that work.
    #[test]
    fn a_second_init_keeps_an_edited_file_as_it_is() {
        let data_dir = a_temp_dir();

        scaffold(&data_dir).unwrap();
        std::fs::write(data_dir.join("jobs/hello.yaml"), "id: mine\nname: Mine\n").unwrap();

        let files = scaffold(&data_dir).unwrap();

        let job = files.iter().find(|file| file.relative_path == "jobs/hello.yaml").unwrap();
        assert!(!job.created);
        assert_eq!(
            std::fs::read_to_string(data_dir.join("jobs/hello.yaml")).unwrap(),
            "id: mine\nname: Mine\n",
        );
    }

    /// The config is a map of what can be set, not a set of settings: an uncommented key
    /// in it would be a default written twice, and the copy on disk would be the one that
    /// goes stale.
    #[test]
    fn the_example_config_is_comments_only() {
        let data_dir = a_temp_dir();

        scaffold(&data_dir).unwrap();

        let config = std::fs::read_to_string(data_dir.join("config.toml")).unwrap();

        for line in config.lines() {
            let line = line.trim();
            assert!(line.is_empty() || line.starts_with('#'), "active line in config.toml: {line}");
        }
    }

    /// The shape the MCP tool hands a model: the directory it acted on, and each file by
    /// the path it was written at rather than by this struct's field name.
    #[test]
    fn the_result_an_agent_reads_names_the_directory_and_each_file_by_path() {
        let data_dir = a_temp_dir();

        let result = InitResult {
            data_dir: data_dir.display().to_string(),
            files: scaffold(&data_dir).unwrap(),
        };

        let json = serde_json::to_value(&result).unwrap();

        assert_eq!(json["data_dir"], data_dir.display().to_string());
        assert_eq!(json["files"][0]["path"], "jobs/hello.yaml");
        assert_eq!(json["files"][0]["created"], true);
        assert!(json["files"][0].get("relative_path").is_none(), "{json}");
    }
}
