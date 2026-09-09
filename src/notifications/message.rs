use crate::crud::job_run::JobRun;
use crate::crud::task_run::TaskRun;
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams;
use crate::router::app::format;

/// What one notification says, in the two parts every channel has some form of: a line
/// naming it and the text itself.
pub struct NotificationMessage {
    pub subject: String,
    pub body: String,
}

/// One task run that did not succeed, with the attempt that decided it and what that
/// attempt printed. The dashboard is a click away, but an alert that makes you go and
/// look before you know what broke is an alert that gets filtered.
pub struct JobRunFailureTask {
    pub task_run: TaskRun,
    pub attempt: Option<TaskRunAttempt>,
    pub streams: TaskRunAttemptOutputStreams,
}


/// What a job run that did not succeed says. The only kind of message there is today,
/// which is why it lives beside the service rather than behind a trait.
pub fn job_run_failure_message(
    job_run: &JobRun,
    task_runs: &[TaskRun],
    failures: &[JobRunFailureTask],
    max_output_bytes: usize,
) -> NotificationMessage {
    NotificationMessage {
        subject: failure_subject(job_run),
        body: failure_body(job_run, task_runs, failures, max_output_bytes),
    }
}

fn failure_subject(job_run: &JobRun) -> String {
    format!(
        "[flowlite] {} run {} {}",
        job_run.job_name,
        job_run.id,
        format::job_run_word(job_run.status),
    )
}

fn failure_body(
    job_run: &JobRun,
    task_runs: &[TaskRun],
    failures: &[JobRunFailureTask],
    max_output_bytes: usize,
) -> String {

    let mut body = String::new();

    body.push_str(&format!(
        "Job run {} of '{}' ({}) {}.\n\n",
        job_run.id,
        job_run.job_name,
        job_run.job_id,
        format::job_run_word(job_run.status),
    ));

    body.push_str(&run_summary(job_run));

    body.push_str("\nTasks\n\n");
    for task_run in task_runs {
        body.push_str(&format!(
            "  {:<24} {}\n",
            task_run.task_id,
            format::task_run_word(task_run.status),
        ));
    }

    for failure in failures {
        body.push('\n');
        body.push_str(&failure_output(failure, max_output_bytes));
    }

    body.push_str(&format!(
        "\nRun `flowlite job-run logs {}` for every task and attempt.\n",
        job_run.id,
    ));

    body
}

fn run_summary(job_run: &JobRun) -> String {

    let mut summary = String::new();

    summary.push_str(&format!("  {:<12}{}\n", "Job", job_run.job_id));
    summary.push_str(&format!("  {:<12}{}\n", "Run", job_run.id));
    summary.push_str(&format!("  {:<12}{}\n", "Status", format::job_run_word(job_run.status)));

    if let Some(scheduled_at) = job_run.scheduled_at {
        summary.push_str(&format!("  {:<12}{}\n", "Scheduled", format::timestamp(scheduled_at)));
    }

    if let Some(started_at) = job_run.started_at {
        summary.push_str(&format!("  {:<12}{}\n", "Started", format::timestamp(started_at)));
    }

    if let Some(finished_at) = job_run.finished_at {
        summary.push_str(&format!("  {:<12}{}\n", "Finished", format::timestamp(finished_at)));
    }

    if let (Some(started_at), Some(finished_at)) = (job_run.started_at, job_run.finished_at) {
        let seconds = finished_at.signed_duration_since(started_at).num_seconds();
        summary.push_str(&format!("  {:<12}{}\n", "Duration", format::duration(seconds)));
    }

    if !job_run.parameters.0.is_empty() {
        summary.push_str(&format!("  {:<12}", "Parameters"));

        let parameters: Vec<String> = job_run.parameters.0
            .iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect();

        summary.push_str(&format!("{}\n", parameters.join(" ")));
    }

    summary
}

fn failure_output(failure: &JobRunFailureTask, max_output_bytes: usize) -> String {

    let mut section = String::new();

    match &failure.attempt {
        Some(attempt) => section.push_str(&format!(
            "Output of {}, attempt {} of {}\n\n",
            failure.task_run.task_id,
            attempt.attempt,
            failure.task_run.max_retries + 1,
        )),
        // A task run that never got an attempt still explains the run: it is why the
        // tasks below it were skipped.
        None => section.push_str(&format!(
            "{} {}, with no attempt\n",
            failure.task_run.task_id,
            format::task_run_word(failure.task_run.status),
        )),
    }

    if failure.attempt.is_none() {
        return section;
    }

    section.push_str("stdout:\n");
    section.push_str(&stream_tail(&failure.streams.stdout, max_output_bytes));
    section.push_str("stderr:\n");
    section.push_str(&stream_tail(&failure.streams.stderr, max_output_bytes));

    section
}

/// The **end** of a stream, which is where a command says why it stopped. Cutting the
/// front is what makes a capped alert still worth reading.
fn stream_tail(stream: &str, max_output_bytes: usize) -> String {

    if stream.is_empty() {
        return "(nothing written)\n".to_string();
    }

    let stream = stream.trim_end();

    if stream.len() <= max_output_bytes {
        return format!("{}\n", stream);
    }

    let mut cut = stream.len() - max_output_bytes;

    while cut < stream.len() && !stream.is_char_boundary(cut) {
        cut += 1;
    }

    format!(
        "(earlier output cut, last {} bytes follow)\n{}\n",
        stream.len() - cut,
        &stream[cut..],
    )
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use chrono::Utc;
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;

    fn job_run(status: JobRunStatus) -> JobRun {
        JobRun {
            id: 42,
            job_id: "nightly-sync".to_string(),
            job_name: "Nightly Sync".to_string(),
            job_description: String::new(),
            parameters: sqlx::types::Json(BTreeMap::new()),
            on_failure_emails: sqlx::types::Json(vec!["oncall@example.com".to_string()]),
            created_at: Utc::now(),
            scheduled_at: None,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            status,
        }
    }

    fn task_run(task_id: &str, status: TaskRunStatus) -> TaskRun {
        TaskRun {
            id: 11,
            job_run_id: 42,
            job_id: "nightly-sync".to_string(),
            task_id: task_id.to_string(),
            command: "./sync.sh".to_string(),
            depends_on: sqlx::types::Json(Vec::new()),
            timeout: 3600,
            max_retries: 2,
            retry_delay: 60,
            env: sqlx::types::Json(BTreeMap::new()),
            working_dir: String::new(),
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            status,
        }
    }

    fn attempt(number: u32) -> TaskRunAttempt {
        TaskRunAttempt {
            id: 5,
            task_run_id: 11,
            job_run_id: 42,
            job_id: "nightly-sync".to_string(),
            task_id: "transform".to_string(),
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            attempt: number,
            status: TaskRunAttemptStatus::Failed,
        }
    }

    #[test]
    fn the_subject_names_the_job_the_run_and_what_happened() {
        assert_eq!(
            failure_subject(&job_run(JobRunStatus::TimedOut)),
            "[flowlite] Nightly Sync run 42 timed out",
        );
    }

    /// What the alert is for: the failing task's own output, in the message, so nobody has
    /// to open the dashboard to find out what broke.
    #[test]
    fn the_body_carries_every_task_and_the_output_of_the_one_that_broke() {

        let task_runs = vec![
            task_run("extract", TaskRunStatus::Succeeded),
            task_run("transform", TaskRunStatus::Failed),
            task_run("load", TaskRunStatus::Skipped),
        ];

        let failures = vec![JobRunFailureTask {
            task_run: task_run("transform", TaskRunStatus::Failed),
            attempt: Some(attempt(3)),
            streams: TaskRunAttemptOutputStreams {
                stdout: "reading rows\n".to_string(),
                stderr: "psycopg2.OperationalError: connection refused\n".to_string(),
            },
        }];

        let body = failure_body(&job_run(JobRunStatus::Failed), &task_runs, &failures, 4096);

        assert!(body.contains("Job run 42 of 'Nightly Sync' (nightly-sync) failed."), "{}", body);

        assert!(body.contains("extract"), "{}", body);
        assert!(body.contains("load"), "{}", body);
        assert!(body.contains("skipped"), "{}", body);

        assert!(body.contains("Output of transform, attempt 3 of 3"), "{}", body);
        assert!(body.contains("psycopg2.OperationalError: connection refused"), "{}", body);

        assert!(body.contains("flowlite job-run logs 42"), "{}", body);
    }

    /// A task that never got an attempt still belongs in the message: it is why the tasks
    /// under it never ran. It just has no output to quote.
    #[test]
    fn a_failed_task_with_no_attempt_says_so_rather_than_quoting_nothing() {

        let failures = vec![JobRunFailureTask {
            task_run: task_run("transform", TaskRunStatus::Failed),
            attempt: None,
            streams: TaskRunAttemptOutputStreams::default(),
        }];

        let body = failure_body(&job_run(JobRunStatus::Failed), &[], &failures, 4096);

        assert!(body.contains("transform failed, with no attempt"), "{}", body);
        assert!(!body.contains("stdout:"), "{}", body);
    }

    #[test]
    fn a_short_stream_is_written_whole() {
        assert_eq!(stream_tail("all fine\n", 100), "all fine\n");
    }

    #[test]
    fn an_empty_stream_says_so_rather_than_being_blank() {
        assert_eq!(stream_tail("", 100), "(nothing written)\n");
    }

    /// The end is what says why the command stopped, so it is the end that survives.
    #[test]
    fn a_long_stream_keeps_its_end() {
        let tail = stream_tail("0123456789abcdef", 6);

        assert!(tail.ends_with("abcdef\n"));
        assert!(tail.starts_with("(earlier output cut, last 6 bytes follow)\n"));
    }

    #[test]
    fn a_cut_never_splits_a_character() {
        // Four bytes each, so a cut of 6 bytes lands mid-character and has to move on.
        let tail = stream_tail("🙂🙂🙂", 6);

        assert!(tail.ends_with("🙂\n"));
    }
}
