use askama::Template;

use crate::crud::job_run::{JobRun, JobRunStatus};
use crate::crud::task_run::{TaskRun, TaskRunStatus};
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams;
use crate::router::app::format;

/// What one notification says: the two parts every channel has some form of - a line
/// naming it and the text itself - and the same thing rendered as HTML for the channels
/// whose transport can show one.
///
/// `html` is optional because a channel that cannot use it simply ignores it, and because
/// a message kind need not have an HTML rendering at all. Email then sends the text part
/// alone rather than an empty alternative.
pub struct NotificationMessage {
    pub subject: String,
    pub body: String,
    pub html: Option<String>,
}

/// One task run that did not succeed, with the attempt that decided it and what that
/// attempt printed. The dashboard is a click away, but an alert that makes you go and
/// look before you know what broke is an alert that gets filtered.
pub struct JobRunFailureTask {
    pub task_run: TaskRun,
    pub attempt: Option<TaskRunAttempt>,
    pub streams: TaskRunAttemptOutputStreams,
}

#[derive(Template)]
#[template(path = "notifications/job_run.html")]
struct JobRunTemplate {
    lead: String,
    status_word: &'static str,
    accent: &'static str,
    summary: Vec<SummaryRow>,
    tasks: Vec<TaskRow>,
    failures: Vec<FailureSection>,
    logs_command: String,
}

struct SummaryRow {
    label: &'static str,
    value: String,
}

struct TaskRow {
    task_id: String,
    status_word: &'static str,
    accent: &'static str,
}

/// `streams` is `None` for a task run that never got an attempt: it belongs in the
/// message because it is why the tasks under it were skipped, but it printed nothing.
struct FailureSection {
    title: String,
    streams: Option<FailureStreams>,
}

struct FailureStreams {
    stdout: String,
    stderr: String,
}


/// What a finished job run says, however it ended. One message shape rather than one per
/// ending: a success and a failure both answer "what did this run do, and what did each of
/// its tasks do", and the only difference is that a failure has output worth quoting.
///
/// The status supplies the wording throughout, so a succeeded run reads as one rather than
/// as a failure notice with the word swapped.
pub fn job_run_message(
    job_run: &JobRun,
    task_runs: &[TaskRun],
    failures: &[JobRunFailureTask],
    max_output_bytes: usize,
) -> NotificationMessage {
    NotificationMessage {
        subject: message_subject(job_run),
        body: message_body(job_run, task_runs, failures, max_output_bytes),
        html: message_html(job_run, task_runs, failures, max_output_bytes),
    }
}

fn message_subject(job_run: &JobRun) -> String {
    format!(
        "[flowlite] {} run {} {}",
        job_run.job_name,
        job_run.id,
        format::job_run_word(job_run.status),
    )
}

/// The sentence both renderings open with, so neither can end up saying something the
/// other does not.
fn message_lead(job_run: &JobRun) -> String {
    format!(
        "Job run {} of '{}' ({}) {}.",
        job_run.id,
        job_run.job_name,
        job_run.job_id,
        format::job_run_word(job_run.status),
    )
}

fn logs_command(job_run: &JobRun) -> String {
    format!("flowlite job-run logs {}", job_run.id)
}

fn message_body(
    job_run: &JobRun,
    task_runs: &[TaskRun],
    failures: &[JobRunFailureTask],
    max_output_bytes: usize,
) -> String {

    let mut body = String::new();

    body.push_str(&format!("{}\n\n", message_lead(job_run)));

    body.push_str(&run_summary(job_run));

    body.push_str("\nTasks\n\n");
    for task_run in task_runs {
        body.push_str(&format!(
            "  {:<24} {}\n",
            task_run.task_id,
            format::task_run_word(task_run.status),
        ));
    }

    // A run that succeeded has none, so this is where the two endings differ and the
    // only place they do.
    for failure in failures {
        body.push('\n');
        body.push_str(&failure_output(failure, max_output_bytes));
    }

    body.push_str(&format!(
        "\nRun `{}` for every task and attempt.\n",
        logs_command(job_run),
    ));

    body
}

/// The same message as HTML, for a channel whose transport can show one.
///
/// Askama escapes every value it writes, which is what makes it safe to quote a command's
/// own output here: a task free to print `<script>` prints it as text.
fn message_html(
    job_run: &JobRun,
    task_runs: &[TaskRun],
    failures: &[JobRunFailureTask],
    max_output_bytes: usize,
) -> Option<String> {

    let summary = summary_rows(job_run)
        .into_iter()
        .map(|(label, value)| SummaryRow { label, value })
        .collect();

    let tasks = task_runs
        .iter()
        .map(|task_run| TaskRow {
            task_id: task_run.task_id.clone(),
            status_word: format::task_run_word(task_run.status),
            accent: task_run_accent(task_run.status),
        })
        .collect();

    let failures = failures
        .iter()
        .map(|failure| FailureSection {
            title: failure_title(failure),
            streams: failure.attempt.as_ref().map(|_| FailureStreams {
                stdout: stream_tail(&failure.streams.stdout, max_output_bytes),
                stderr: stream_tail(&failure.streams.stderr, max_output_bytes),
            }),
        })
        .collect();

    let template = JobRunTemplate {
        lead: message_lead(job_run),
        status_word: format::job_run_word(job_run.status),
        accent: job_run_accent(job_run.status),
        summary,
        tasks,
        failures,
        logs_command: logs_command(job_run),
    };

    match template.render() {
        Ok(html) => Some(html),
        // The text part is already on the message, so a template that will not render
        // costs the formatting rather than the notification.
        Err(e) => {
            eprintln!("Template rendering error: {}", e);
            None
        }
    }
}

/// The facts a finished run's summary shows, in the order it shows them. One list for
/// both renderings, so a fact added to one cannot go missing from the other.
fn summary_rows(job_run: &JobRun) -> Vec<(&'static str, String)> {

    let mut rows = vec![
        ("Job", job_run.job_id.clone()),
        ("Run", job_run.id.to_string()),
        ("Status", format::job_run_word(job_run.status).to_string()),
    ];

    if let Some(scheduled_at) = job_run.scheduled_at {
        rows.push(("Scheduled", format::timestamp(scheduled_at)));
    }

    if let Some(started_at) = job_run.started_at {
        rows.push(("Started", format::timestamp(started_at)));
    }

    if let Some(finished_at) = job_run.finished_at {
        rows.push(("Finished", format::timestamp(finished_at)));
    }

    if let (Some(started_at), Some(finished_at)) = (job_run.started_at, job_run.finished_at) {
        let seconds = finished_at.signed_duration_since(started_at).num_seconds();
        rows.push(("Duration", format::duration(seconds)));
    }

    if !job_run.parameters.0.is_empty() {
        let parameters: Vec<String> = job_run.parameters.0
            .iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect();

        rows.push(("Parameters", parameters.join(" ")));
    }

    rows
}

fn run_summary(job_run: &JobRun) -> String {

    let mut summary = String::new();

    for (label, value) in summary_rows(job_run) {
        summary.push_str(&format!("  {:<12}{}\n", label, value));
    }

    summary
}

fn failure_title(failure: &JobRunFailureTask) -> String {

    match &failure.attempt {
        Some(attempt) => format!(
            "Output of {}, attempt {} of {}",
            failure.task_run.task_id,
            attempt.attempt,
            failure.task_run.max_retries + 1,
        ),
        // A task run that never got an attempt still explains the run: it is why the
        // tasks below it were skipped.
        None => format!(
            "{} {}, with no attempt",
            failure.task_run.task_id,
            format::task_run_word(failure.task_run.status),
        ),
    }
}

fn failure_output(failure: &JobRunFailureTask, max_output_bytes: usize) -> String {

    let title = failure_title(failure);

    if failure.attempt.is_none() {
        return format!("{}\n", title);
    }

    let mut section = format!("{}\n\n", title);

    section.push_str("stdout:\n");
    section.push_str(&stream_tail(&failure.streams.stdout, max_output_bytes));
    section.push_str("stderr:\n");
    section.push_str(&stream_tail(&failure.streams.stderr, max_output_bytes));

    section
}

/// The colour a status is shown in, written into the message itself: a mail client
/// fetches no stylesheet, so a class would arrive unstyled.
fn job_run_accent(status: JobRunStatus) -> &'static str {
    match status {
        JobRunStatus::Pending => "#6a737d",
        JobRunStatus::Running => "#0969da",
        JobRunStatus::Succeeded => "#1a7f37",
        JobRunStatus::Failed => "#b42318",
        JobRunStatus::Skipped => "#6a737d",
        JobRunStatus::Aborted => "#b54708",
        JobRunStatus::TimedOut => "#b42318",
    }
}

fn task_run_accent(status: TaskRunStatus) -> &'static str {
    match status {
        TaskRunStatus::Pending => "#6a737d",
        TaskRunStatus::Running => "#0969da",
        TaskRunStatus::Succeeded => "#1a7f37",
        TaskRunStatus::Failed => "#b42318",
        TaskRunStatus::Skipped => "#6a737d",
        TaskRunStatus::Aborted => "#b54708",
        TaskRunStatus::TimedOut => "#b42318",
    }
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
            message_subject(&job_run(JobRunStatus::TimedOut)),
            "[flowlite] Nightly Sync run 42 timed out",
        );
    }

    /// The same line for the good news, so a success reads as one instead of as a failure
    /// notice with a word swapped.
    #[test]
    fn a_succeeded_run_says_so_in_its_own_subject() {
        assert_eq!(
            message_subject(&job_run(JobRunStatus::Succeeded)),
            "[flowlite] Nightly Sync run 42 succeeded",
        );
    }

    /// A success is worth reading for the same reasons a failure is — how long it took and
    /// what each task did — and it has no output to quote, so the body is the summary and
    /// the task list alone.
    #[test]
    fn a_succeeded_run_carries_its_tasks_and_quotes_no_output() {

        let task_runs = vec![
            task_run("extract", TaskRunStatus::Succeeded),
            task_run("load", TaskRunStatus::Succeeded),
        ];

        let body = message_body(&job_run(JobRunStatus::Succeeded), &task_runs, &[], 4096);

        assert!(body.contains("Job run 42 of 'Nightly Sync' (nightly-sync) succeeded."), "{}", body);

        assert!(body.contains("extract"), "{}", body);
        assert!(body.contains("load"), "{}", body);

        assert!(!body.contains("stdout:"), "{}", body);
        assert!(!body.contains("Output of"), "{}", body);
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

        let body = message_body(&job_run(JobRunStatus::Failed), &task_runs, &failures, 4096);

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

        let body = message_body(&job_run(JobRunStatus::Failed), &[], &failures, 4096);

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

    fn failure(stdout: &str, stderr: &str) -> JobRunFailureTask {
        JobRunFailureTask {
            task_run: task_run("transform", TaskRunStatus::Failed),
            attempt: Some(attempt(3)),
            streams: TaskRunAttemptOutputStreams {
                stdout: stdout.to_string(),
                stderr: stderr.to_string(),
            },
        }
    }

    #[test]
    fn the_html_names_the_run_and_every_task() {

        let task_runs = vec![
            task_run("extract", TaskRunStatus::Succeeded),
            task_run("load", TaskRunStatus::Skipped),
        ];

        let html = message_html(&job_run(JobRunStatus::Failed), &task_runs, &[], 4096).unwrap();

        assert!(html.contains("Nightly Sync"), "{}", html);
        assert!(html.contains("extract"), "{}", html);
        assert!(html.contains("load"), "{}", html);
        assert!(html.contains("flowlite job-run logs 42"), "{}", html);
    }

    /// The same rule the text body follows: the section that quotes output is not written
    /// when there is none, which is what makes one template enough for both endings.
    #[test]
    fn a_succeeded_run_quotes_no_output_in_its_html() {

        let task_runs = vec![task_run("extract", TaskRunStatus::Succeeded)];

        let html = message_html(&job_run(JobRunStatus::Succeeded), &task_runs, &[], 4096).unwrap();

        assert!(html.contains("succeeded"), "{}", html);
        assert!(!html.contains("<pre"), "{}", html);
        assert!(!html.contains("stderr"), "{}", html);
    }

    #[test]
    fn the_html_carries_the_failing_tasks_output() {

        let failures = vec![failure(
            "reading rows\n",
            "psycopg2.OperationalError: connection refused\n",
        )];

        let html = message_html(&job_run(JobRunStatus::Failed), &[], &failures, 4096).unwrap();

        assert!(html.contains("Output of transform, attempt 3 of 3"), "{}", html);
        assert!(html.contains("psycopg2.OperationalError: connection refused"), "{}", html);
    }

    /// A command is free to print markup, and it has to arrive as text rather than as
    /// part of the message.
    #[test]
    fn markup_printed_by_a_command_is_escaped_rather_than_rendered() {

        let failures = vec![failure("", "<script>alert('x')</script> a & b\n")];

        let html = message_html(&job_run(JobRunStatus::Failed), &[], &failures, 4096).unwrap();

        assert!(!html.contains("<script>"), "{}", html);
        assert!(!html.contains("</script>"), "{}", html);
        assert!(html.contains("&#60;script&#62;"), "{}", html);
        assert!(html.contains("a &#38; b"), "{}", html);
    }

    /// The cap is one rule for both renderings: a stream too long for the text body is
    /// too long for the HTML one.
    #[test]
    fn the_html_cuts_a_long_stream_the_way_the_text_body_does() {

        let failures = vec![failure("", "0123456789abcdef")];

        let html = message_html(&job_run(JobRunStatus::Failed), &[], &failures, 6).unwrap();

        assert!(html.contains("earlier output cut"), "{}", html);
        assert!(!html.contains("0123456789"), "{}", html);
    }

    /// A task that never got an attempt has nothing to quote in either rendering.
    #[test]
    fn a_failed_task_with_no_attempt_says_so_in_the_html_too() {

        let failures = vec![JobRunFailureTask {
            task_run: task_run("transform", TaskRunStatus::Failed),
            attempt: None,
            streams: TaskRunAttemptOutputStreams::default(),
        }];

        let html = message_html(&job_run(JobRunStatus::Failed), &[], &failures, 4096).unwrap();

        assert!(html.contains("transform failed, with no attempt"), "{}", html);
        assert!(!html.contains("<pre"), "{}", html);
    }

    /// Both renderings on the one message, so a client that prefers plain text loses
    /// nothing by the HTML part existing.
    #[test]
    fn a_job_run_message_carries_both_renderings() {

        let message = job_run_message(&job_run(JobRunStatus::Failed), &[], &[], 4096);

        assert!(message.body.contains("Job run 42"), "{}", message.body);
        assert!(message.html.unwrap().contains("Job run 42"));
    }
}
