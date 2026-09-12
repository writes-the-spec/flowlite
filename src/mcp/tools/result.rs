//! What a tool hands back, whichever tool it was: the JSON, the wording an error reaches
//! the caller in, and the warning that a directory nothing serves will not move a run
//! along.

use std::path::Path;

use rmcp::model::{CallToolResult, ContentBlock};
use serde::Serialize;

use crate::crud::job_run::JobRun;
use crate::serve_state::{status, ServeStatus};
use crate::shared::wait::DataDirNotServed;

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
pub(super) fn success_json(value: impl Serialize) -> CallToolResult {
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
pub(super) fn error_result(err: &anyhow::Error) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(format!("{err:#}"))])
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
pub(super) fn job_run_result(job_run: JobRun, warning: Option<String>) -> CallToolResult {
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
pub(super) fn unserved_directory_warning(data_dir: &str) -> Option<String> {
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

/// Turns `ensure_data_dir_is_served`'s typed refusal into these tools' own words: nothing
/// here was a flag to drop, so the remedy names `wait_seconds`, the argument the caller
/// actually sent. The CLI's wording of the same fact - "drop --wait" - would send an agent
/// looking for a flag no tool takes, whose only repairs are to retry unchanged or to invent
/// one. Any other error passes through unchanged.
///
/// Here rather than in one of the three tools that wait, because all three reword it and
/// the wording is what the agent's next move is read off.
pub(super) fn describe_unserved_data_dir(err: anyhow::Error) -> anyhow::Error {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::crud::job_run::JobRunStatus;

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
}
