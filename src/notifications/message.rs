use askama::Template;

use crate::crud::job_run::{JobRun, JobRunStatus};
use crate::crud::task_run::{TaskRun, TaskRunStatus};
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams;
use crate::notifications::slack;
use crate::router::app::format;

/// What one notification says: the two parts every channel has some form of - a line
/// naming it and the text itself - and the same thing rendered again for the channels
/// whose transport can show more than text. Email reads `html` and ignores `blocks`,
/// Slack reads `blocks` and ignores `html`.
///
/// Both are optional because a channel that cannot use one simply ignores it, and because
/// a message kind need not have that rendering at all. Email then sends the text part
/// alone rather than an empty alternative, and Slack posts it fenced.
pub struct NotificationMessage {
    pub subject: String,
    pub body: String,
    pub html: Option<String>,
    pub blocks: Option<serde_json::Value>,
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
        blocks: Some(message_blocks(job_run, task_runs, failures, max_output_bytes)),
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

/// Slack's own limits on one message. It refuses a message that exceeds any of them
/// rather than trimming it, so the trimming happens here.
const MAX_BLOCKS: usize = 50;
const MAX_HEADER_CHARS: usize = 150;
const MAX_SECTION_CHARS: usize = 3000;
const MAX_FIELD_CHARS: usize = 2000;


/// The same message as Slack Block Kit, for the one transport that lays out structure of
/// its own rather than being handed markup.
///
/// Built here beside the text and the HTML, from the same facts, for the reason those two
/// are: a fact added to one rendering cannot then go missing from another. What is Slack's
/// own — how its text is escaped and fenced — stays in `slack.rs` with the transport.
fn message_blocks(
    job_run: &JobRun,
    task_runs: &[TaskRun],
    failures: &[JobRunFailureTask],
    max_output_bytes: usize,
) -> serde_json::Value {

    let mut blocks = vec![header_block(&format!(
        "{} {} run {} {}",
        job_run_emoji(job_run.status),
        job_run.job_name,
        job_run.id,
        format::job_run_word(job_run.status),
    ))];

    let fields = summary_rows(job_run)
        .into_iter()
        .map(|(label, value)| format!("*{}*\n{}", label, slack::escape(&value)))
        .collect();

    blocks.push(fields_block(fields));

    blocks.extend(task_sections(task_runs));

    // A run that succeeded has none, so this is where the two endings differ and the
    // only place they do.
    for failure in failures {
        blocks.push(section_block(&format!("*{}*", slack::escape(&failure_title(failure)))));

        if failure.attempt.is_some() {
            blocks.push(section_block(&fenced(
                "stdout",
                &stream_tail(&failure.streams.stdout, max_output_bytes),
            )));

            blocks.push(section_block(&fenced(
                "stderr",
                &stream_tail(&failure.streams.stderr, max_output_bytes),
            )));
        }
    }

    blocks.push(context_block(&format!(
        "Every task and attempt: `{}`",
        logs_command(job_run),
    )));

    // Whatever overran — a run that failed in thirty tasks at once, or one with more
    // tasks than fit — is cut from the end, and the line naming the command is written
    // again as the last block: the point of cutting is that what is left still says
    // where the rest of it is.
    if blocks.len() > MAX_BLOCKS {
        blocks.truncate(MAX_BLOCKS - 1);

        blocks.push(context_block(&format!(
            "Too long for one message. Everything it left out: `{}`",
            logs_command(job_run),
        )));
    }

    serde_json::Value::Array(blocks)
}

/// The task list, split across as many sections as its length needs. One section per task
/// would spend the block budget on a handful of tasks, and one section for all of them is
/// refused as soon as a job has a few hundred.
fn task_sections(task_runs: &[TaskRun]) -> Vec<serde_json::Value> {

    if task_runs.is_empty() {
        return Vec::new();
    }

    let mut sections = Vec::new();
    let mut section = String::from("*Tasks*");

    for task_run in task_runs {
        let line = format!(
            "\n{} `{}` — {}",
            task_run_emoji(task_run.status),
            slack::escape(&task_run.task_id),
            format::task_run_word(task_run.status),
        );

        if section.chars().count() + line.chars().count() > MAX_SECTION_CHARS {
            sections.push(section_block(section.trim_start()));
            section = String::new();
        }

        section.push_str(&line);
    }

    sections.push(section_block(section.trim_start()));

    sections
}

/// A stream under its own label, in a code block, because it is output that only reads in
/// a monospaced one — and because a fence is what stops Slack reading a `*` a command
/// printed as formatting.
///
/// Cut to fit around the fence rather than after it: trimming the finished section would
/// take the closing fence off and leave the rest of the message inside the code block.
fn fenced(label: &str, stream: &str) -> String {

    let stream = slack::fence_safe(&slack::escape(stream.trim_end()));

    let wrapper = format!("*{}*\n```\n\n```", label);

    format!(
        "*{}*\n```\n{}\n```",
        label,
        truncated(&stream, MAX_SECTION_CHARS - wrapper.chars().count()),
    )
}

fn truncated(text: &str, max_chars: usize) -> String {

    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let kept: String = text.chars().take(max_chars - 1).collect();

    format!("{}…", kept)
}

fn header_block(text: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": truncated(text, MAX_HEADER_CHARS),
            "emoji": true,
        },
    })
}

fn section_block(mrkdwn: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "section",
        "text": { "type": "mrkdwn", "text": truncated(mrkdwn, MAX_SECTION_CHARS) },
    })
}

fn fields_block(fields: Vec<String>) -> serde_json::Value {

    let fields: Vec<serde_json::Value> = fields
        .into_iter()
        .map(|field| serde_json::json!({
            "type": "mrkdwn",
            "text": truncated(&field, MAX_FIELD_CHARS),
        }))
        .collect();

    serde_json::json!({ "type": "section", "fields": fields })
}

fn context_block(mrkdwn: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "context",
        "elements": [{ "type": "mrkdwn", "text": truncated(mrkdwn, MAX_SECTION_CHARS) }],
    })
}

/// What a status looks like in Slack, where a colour cannot be set on a line of text.
/// Exhaustive beside the accents, so a new status has to say how it reads in every
/// rendering rather than defaulting to one of the others.
fn job_run_emoji(status: JobRunStatus) -> &'static str {
    match status {
        JobRunStatus::Pending => ":hourglass_flowing_sand:",
        JobRunStatus::Running => ":arrows_counterclockwise:",
        JobRunStatus::Succeeded => ":white_check_mark:",
        JobRunStatus::Failed => ":x:",
        JobRunStatus::Skipped => ":heavy_minus_sign:",
        JobRunStatus::Aborted => ":octagonal_sign:",
        JobRunStatus::TimedOut => ":alarm_clock:",
        JobRunStatus::Invalid => ":warning:",
    }
}

fn task_run_emoji(status: TaskRunStatus) -> &'static str {
    match status {
        TaskRunStatus::Pending => ":hourglass_flowing_sand:",
        TaskRunStatus::Running => ":arrows_counterclockwise:",
        TaskRunStatus::Succeeded => ":white_check_mark:",
        TaskRunStatus::Failed => ":x:",
        TaskRunStatus::Skipped => ":heavy_minus_sign:",
        TaskRunStatus::Aborted => ":octagonal_sign:",
        TaskRunStatus::TimedOut => ":alarm_clock:",
        TaskRunStatus::Invalid => ":warning:",
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
        JobRunStatus::Invalid => "#6f42c1",
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
        TaskRunStatus::Invalid => "#6f42c1",
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

    /// Every rendering on the one message, so no channel is left with a shape its
    /// transport cannot show.
    #[test]
    fn a_job_run_message_carries_every_rendering() {

        let message = job_run_message(&job_run(JobRunStatus::Failed), &[], &[], 4096);

        assert!(message.body.contains("Job run 42"), "{}", message.body);
        assert!(message.html.unwrap().contains("Job run 42"));

        let blocks = message.blocks.unwrap();

        assert_eq!(blocks[0]["type"], "header");
        assert!(blocks_text(&blocks).contains("Nightly Sync run 42 failed"), "{}", blocks);
    }

    /// Everything the blocks say, for the assertions that only care that a word is or is
    /// not somewhere in the post.
    fn blocks_text(blocks: &serde_json::Value) -> String {
        serde_json::to_string(blocks).unwrap()
    }

    fn blocks_of(
        job_run: &JobRun,
        task_runs: &[TaskRun],
        failures: &[JobRunFailureTask],
        max_output_bytes: usize,
    ) -> Vec<serde_json::Value> {
        message_blocks(job_run, task_runs, failures, max_output_bytes)
            .as_array()
            .unwrap()
            .clone()
    }

    #[test]
    fn the_blocks_open_with_a_header_naming_the_run_and_what_happened() {

        let blocks = blocks_of(&job_run(JobRunStatus::Failed), &[], &[], 4096);

        assert_eq!(blocks[0]["type"], "header");
        assert_eq!(blocks[0]["text"]["type"], "plain_text");

        let header = blocks[0]["text"]["text"].as_str().unwrap();

        assert!(header.contains("Nightly Sync"), "{}", header);
        assert!(header.contains("failed"), "{}", header);
        assert!(header.contains(":x:"), "{}", header);
    }

    /// The summary is two columns of fields rather than the text body's aligned block:
    /// Slack lays fields out itself, and padding to a column width only reads in a fence.
    #[test]
    fn the_summary_becomes_fields_rather_than_an_aligned_column() {

        let blocks = blocks_of(&job_run(JobRunStatus::Succeeded), &[], &[], 4096);

        let section = blocks
            .iter()
            .find(|block| block["fields"].is_array())
            .expect("a section carrying the summary as fields");

        let fields: Vec<String> = section["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field["text"].as_str().unwrap().to_string())
            .collect();

        assert!(fields.iter().any(|field| field.contains("*Job*") && field.contains("nightly-sync")), "{:?}", fields);
        assert!(fields.iter().any(|field| field.contains("*Run*") && field.contains("42")), "{:?}", fields);
    }

    #[test]
    fn every_task_is_listed_with_its_status() {

        let task_runs = vec![
            task_run("extract", TaskRunStatus::Succeeded),
            task_run("load", TaskRunStatus::Skipped),
        ];

        let text = blocks_text(&message_blocks(&job_run(JobRunStatus::Failed), &task_runs, &[], 4096));

        assert!(text.contains("extract"), "{}", text);
        assert!(text.contains("load"), "{}", text);
        assert!(text.contains("skipped"), "{}", text);
    }

    /// The same rule both other renderings follow: the section that quotes output is not
    /// written when there is none.
    #[test]
    fn a_succeeded_run_quotes_no_output_in_its_blocks() {

        let task_runs = vec![task_run("extract", TaskRunStatus::Succeeded)];

        let text = blocks_text(&message_blocks(&job_run(JobRunStatus::Succeeded), &task_runs, &[], 4096));

        assert!(!text.contains("```"), "{}", text);
        assert!(!text.contains("stderr"), "{}", text);
    }

    #[test]
    fn the_blocks_carry_the_failing_tasks_output_in_a_fence() {

        let failures = vec![failure(
            "reading rows\n",
            "psycopg2.OperationalError: connection refused\n",
        )];

        let text = blocks_text(&message_blocks(&job_run(JobRunStatus::Failed), &[], &failures, 4096));

        assert!(text.contains("Output of transform, attempt 3 of 3"), "{}", text);
        assert!(text.contains("psycopg2.OperationalError: connection refused"), "{}", text);
        assert!(text.contains("```"), "{}", text);
    }

    #[test]
    fn a_failed_task_with_no_attempt_says_so_in_the_blocks_too() {

        let failures = vec![JobRunFailureTask {
            task_run: task_run("transform", TaskRunStatus::Failed),
            attempt: None,
            streams: TaskRunAttemptOutputStreams::default(),
        }];

        let text = blocks_text(&message_blocks(&job_run(JobRunStatus::Failed), &[], &failures, 4096));

        assert!(text.contains("transform failed, with no attempt"), "{}", text);
        assert!(!text.contains("```"), "{}", text);
    }

    #[test]
    fn the_blocks_end_with_the_command_that_shows_everything() {

        let blocks = blocks_of(&job_run(JobRunStatus::Failed), &[], &[], 4096);

        let last = blocks.last().unwrap();

        assert_eq!(last["type"], "context");
        assert!(
            last["elements"][0]["text"].as_str().unwrap().contains("flowlite job-run logs 42"),
            "{}",
            last,
        );
    }

    /// A task is free to print a `<`, and Slack reads an unescaped one as the start of
    /// something it then swallows along with whatever follows it.
    #[test]
    fn markup_printed_by_a_command_is_escaped_in_the_blocks_too() {

        let failures = vec![failure("", "expected a < b & c\n")];

        let text = blocks_text(&message_blocks(&job_run(JobRunStatus::Failed), &[], &failures, 4096));

        assert!(text.contains("a &lt; b &amp; c"), "{}", text);
    }

    /// A command is free to print a fence of its own, and it must not be able to end the
    /// block its output is quoted in.
    #[test]
    fn a_fence_in_the_quoted_output_cannot_close_the_code_block() {

        let failures = vec![failure("", "boom\n```\nmore\n")];

        let blocks = blocks_of(&job_run(JobRunStatus::Failed), &[], &failures, 4096);

        let stderr = blocks
            .iter()
            .filter_map(|block| block["text"]["text"].as_str())
            .find(|text| text.starts_with("*stderr*"))
            .expect("a section quoting stderr");

        assert_eq!(stderr.matches("```").count(), 2, "{}", stderr);
        assert!(stderr.contains("boom"), "{}", stderr);
        assert!(stderr.contains("more"), "{}", stderr);
    }

    /// Slack refuses a section over 3000 characters outright rather than trimming it, so a
    /// job with a few hundred tasks has to arrive as several sections.
    #[test]
    fn a_long_task_list_is_split_rather_than_refused_by_slack() {

        let task_runs: Vec<TaskRun> = (0..300)
            .map(|n| task_run(&format!("task-{:03}", n), TaskRunStatus::Succeeded))
            .collect();

        let blocks = blocks_of(&job_run(JobRunStatus::Succeeded), &task_runs, &[], 4096);

        for block in &blocks {
            if let Some(text) = block["text"]["text"].as_str() {
                assert!(text.chars().count() <= 3000, "{} characters", text.chars().count());
            }
        }

        let text = blocks_text(&serde_json::Value::Array(blocks));

        assert!(text.contains("task-000"), "the first task is missing");
        assert!(text.contains("task-299"), "the last task is missing");
    }

    /// Escaping grows a stream — every `&` becomes five characters — so a stream inside the
    /// byte cap can still be over the character cap once it is escaped.
    #[test]
    fn an_escaped_stream_too_long_for_a_section_is_cut_to_fit() {

        let failures = vec![failure("", &"&".repeat(2000))];

        let blocks = blocks_of(&job_run(JobRunStatus::Failed), &[], &failures, 4096);

        for block in &blocks {
            if let Some(text) = block["text"]["text"].as_str() {
                assert!(text.chars().count() <= 3000, "{} characters", text.chars().count());
                assert_eq!(text.matches("```").count() % 2, 0, "a fence was left open");
            }
        }
    }

    #[test]
    fn a_very_long_job_name_is_cut_to_fit_slacks_header() {

        let mut job_run = job_run(JobRunStatus::Failed);
        job_run.job_name = "N".repeat(400);

        let blocks = blocks_of(&job_run, &[], &[], 4096);

        let header = blocks[0]["text"]["text"].as_str().unwrap();

        assert!(header.chars().count() <= 150, "{} characters", header.chars().count());
    }

    /// Slack takes 50 blocks and refuses the whole message at 51, which a run that failed
    /// in many tasks at once would otherwise reach.
    #[test]
    fn a_run_with_many_failed_tasks_stays_under_slacks_block_limit() {

        let failures: Vec<JobRunFailureTask> = (0..40)
            .map(|_| failure("reading rows\n", "boom\n"))
            .collect();

        let blocks = blocks_of(&job_run(JobRunStatus::Failed), &[], &failures, 4096);

        assert!(blocks.len() <= 50, "{} blocks", blocks.len());

        let last = blocks.last().unwrap();

        assert!(
            last["elements"][0]["text"].as_str().unwrap().contains("flowlite job-run logs 42"),
            "a cut message still has to say where the rest is: {}",
            last,
        );
    }

}
