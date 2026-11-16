//! `flowlite validate` runs the checks `serve` makes at startup and nothing else: it passes
//! where `serve` would start, refuses where `serve` would refuse, and leaves no database.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BINARY: &str = env!("CARGO_BIN_EXE_flowlite");


fn project_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-validate-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

fn validate(dir: &Path, json: bool) -> Output {
    let mut command = Command::new(BINARY);

    command.arg("-p").arg(dir).arg("validate");

    if json {
        command.arg("--json");
    }

    command.output().unwrap()
}

const HELLO: &str = "id: hello\nname: Hello\ntasks:\n  - id: say\n    command: \"true\"\n";


#[test]
fn a_valid_directory_passes_with_its_counts_and_leaves_no_database() {
    let dir = project_dir("valid");
    std::fs::write(dir.join("jobs").join("hello.yaml"), HELLO).unwrap();

    let output = validate(&dir, false);

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8_lossy(&output.stdout), "1 job, 0 schedules: OK\n");
    assert!(!dir.join(".flowlite").join("flowlite.db").exists(), "validate opened the database");

    let output = validate(&dir, true);
    let counts: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(counts, serde_json::json!({ "jobs": 1, "schedules": 0 }));
}

#[test]
fn a_file_that_does_not_parse_fails_naming_the_file() {
    let dir = project_dir("unparsable");
    std::fs::write(dir.join("jobs").join("broken.yaml"), "name: No Id\ntasks: []\n").unwrap();

    let output = validate(&dir, true);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty(), "an error must not reach stdout");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("broken.yaml"), "{stderr}");
}

/// The check `job list` leaves out on purpose and `serve` makes: `validate` answers for
/// `serve`.
#[test]
fn a_secret_nothing_defines_fails_as_it_would_fail_serve() {
    let dir = project_dir("secret");

    std::fs::write(
        dir.join("jobs").join("nightly.yaml"),
        "id: nightly\n\
         name: Nightly\n\
         tasks:\n\
         \x20 - id: load\n\
         \x20   command: \"true\"\n\
         \x20   secret_env:\n\
         \x20     PGPASSWORD: warehouse_pw\n",
    ).unwrap();

    let output = validate(&dir, false);

    assert_eq!(output.status.code(), Some(1));

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("warehouse_pw"), "{stderr}");
}
