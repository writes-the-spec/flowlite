use std::path::Path;
use clap::Args;
use crate::shared::init::{scaffold, ScaffoldedFile};
use crate::toolkit::Toolkit;


#[derive(Args)]
pub struct InitCmd {
}


impl InitCmd {

    /// Takes no connection and seeds nothing. `init` writes the configuration every other
    /// command reads, so it must work in a directory that has no database yet and in one
    /// whose YAML no longer parses - the two moments it is most likely to be reached for.
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let data_dir = Path::new(&toolkit.app_config.data_dir);

        let files = scaffold(data_dir)?;

        println!("{}", init_report(data_dir, &files));

        Ok(())
    }
}


/// Names the directory because `-D` and `FLOWLITE_DATA_DIR` both mean the scaffold can
/// land somewhere other than where the command was typed.
pub fn init_report(data_dir: &Path, files: &[ScaffoldedFile]) -> String {

    let mut lines = vec![format!("flowlite data directory {}", data_dir.display()), String::new()];

    for file in files {
        let verb = if file.created { "wrote" } else { "kept " };
        lines.push(format!("  {verb} {}", file.relative_path));
    }

    lines.push(String::new());
    lines.push("Run it with:  flowlite job submit hello-world".to_string());
    lines.push("Start the scheduler, the orchestrator and the UI with:  flowlite serve".to_string());

    lines.join("\n")
}


#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use super::*;

    fn a_temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("flowlite-init-report-test-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn the_report_says_which_files_were_written_and_which_were_kept() {
        let data_dir = a_temp_dir();

        let first = scaffold(&data_dir).unwrap();
        let second = scaffold(&data_dir).unwrap();

        let wrote = init_report(&data_dir, &first);
        let kept = init_report(&data_dir, &second);

        assert!(wrote.contains("wrote"), "{wrote}");
        assert!(!wrote.contains("kept"), "{wrote}");
        assert!(kept.contains("kept"), "{kept}");
        assert!(!kept.contains("wrote"), "{kept}");
    }

    #[test]
    fn the_report_names_the_directory_and_the_command_that_starts_it() {
        let data_dir = a_temp_dir();

        let files = scaffold(&data_dir).unwrap();
        let report = init_report(&data_dir, &files);

        assert!(report.contains(&data_dir.display().to_string()), "{report}");
        assert!(report.contains("flowlite serve"), "{report}");
    }
}
