//! The numbers the README prints are the ones the code actually defaults to.
//!
//! Its Configuration section is a table of defaults a reader copies and edits, so a default
//! changed in `AppConfig` and not here makes the README quietly wrong about what the program
//! does — and unlike a broken link, nothing about reading it suggests checking.
//!
//! This compares values, not keys: a key added to a section is not caught, because a README
//! is allowed to be shorter than the struct. What it catches is the drift that matters, a
//! number that used to be true.

use flowlite::app_config::AppConfig;

#[test]
fn the_readme_prints_the_real_defaults() {

    let defaults = AppConfig::default();
    let orchestrator = &defaults.orchestrator;
    let ui = &defaults.ui;
    let job = &defaults.job_defaults;

    // `[smtp]` has no Default - there is no default mail server - so its per-field defaults
    // are what serde fills in for a section naming only the two keys it must.
    let smtp: flowlite::app_config::AppConfigSmtp =
        serde_json::from_str(r#"{"host": "", "from": ""}"#).unwrap();

    // (section, key, the value the README should print) - keyed by section because
    // `timeout_seconds` is a [slack] key and a [job_defaults] one, with different defaults.
    let documented = [
        ("orchestrator", "poll_interval_seconds", orchestrator.poll_interval_seconds.to_string()),
        ("orchestrator", "error_backoff_seconds", orchestrator.error_backoff_seconds.to_string()),
        ("orchestrator", "reader_eof_timeout_seconds", orchestrator.reader_eof_timeout_seconds.to_string()),
        ("orchestrator", "max_stream_bytes", orchestrator.max_stream_bytes.to_string()),
        ("orchestrator", "read_buffer_bytes", orchestrator.read_buffer_bytes.to_string()),
        ("orchestrator", "max_running_attempts", orchestrator.max_running_attempts.to_string()),
        ("ui", "page_size", ui.page_size.to_string()),
        ("ui", "max_page_size", ui.max_page_size.to_string()),
        ("ui", "refresh_interval_seconds", ui.refresh_interval_seconds.to_string()),
        ("job_defaults", "timeout_seconds", job.timeout_seconds.to_string()),
        ("job_defaults", "max_retries", job.max_retries.to_string()),
        ("job_defaults", "retry_delay_seconds", job.retry_delay_seconds.to_string()),
        ("job_defaults", "max_parallel_runs", job.max_parallel_runs.to_string()),
        ("schedule_defaults", "timezone", format!("\"{}\"", defaults.schedule_defaults.timezone)),
        ("smtp", "port", smtp.port.to_string()),
        ("smtp", "max_output_bytes", smtp.max_output_bytes.to_string()),
    ];

    let readme = std::fs::read_to_string("README.md").unwrap();

    let mut wrong = Vec::new();

    for (section, key, value) in documented {
        let mut current = "";
        let mut found = false;

        for line in readme.lines() {
            if let Some(name) = line.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) {
                current = name;
            }

            let Some(printed) = line.strip_prefix(&format!("{key} = ")) else {
                continue;
            };

            if current != section {
                continue;
            }

            found = true;
            let printed = printed.split_whitespace().next().unwrap_or("");

            if printed != value {
                wrong.push(format!("[{section}] {key}: README says {printed}, the default is {value}"));
            }
        }

        if !found {
            wrong.push(format!("[{section}] {key}: not printed in the README"));
        }
    }

    assert!(
        wrong.is_empty(),
        "The README's defaults disagree with AppConfig:\n{}\n\nThe Configuration section is \
         a table people copy. A number that used to be true is worse than one left out.",
        wrong.join("\n"),
    );
}
