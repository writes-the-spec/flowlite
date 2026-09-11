//! `job submit -f <file>` — a run whose definition was never installed in the data
//! directory. Driven through the built binary rather than as unit tests because the whole
//! feature turns on what `mem` holds, and `mem` is one shared-cache database per process:
//! seeding it inside a unit test races every other test's connection over its schema lock
//! (see `src/test_support.rs:70-72`), which is the same reason `tests/serve_secret_check.rs`
//! lives out here.

use std::path::{Path, PathBuf};
use std::time::Duration;

mod common;
use common::{flowlite, install_job, is_up, serve, until, ServerGuard};

/// An empty data directory: no jobs installed, so anything a test runs came from a file.
fn data_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-adhoc-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    dir
}

/// Writes a job file *outside* `jobs/`, so nothing installs it.
fn job_file(dir: &Path, name: &str, yaml: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, yaml).unwrap();
    path
}

/// reqwest is built with `rustls-no-provider`, so a Client cannot be built until one is
/// installed - the same line `src/notifications/slack.rs` runs before it builds its own.
/// Idempotent, so every test that fetches a page can just call it.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

const HELLO: &str = "id: hello\nname: Hello\ntasks:\n  - id: say\n    \
                     description: Says hello\n    command: echo hello\n";

/// The run is written and reported exactly like one of an installed job — the id on stdout
/// is the whole point, since it is the handle everything afterwards needs.
#[test]
fn a_file_is_submitted_and_reports_its_run_id() {
    let dir = data_dir("submits");
    let file = job_file(&dir, "hello.yaml", HELLO);

    let out = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy()]);
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();

    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("Job Run ID"), "{stdout}");
}

/// The definition is read where it lies and never installed: the job must not appear in
/// the data directory's own listing afterwards, or this is just a slow `cp`.
#[test]
fn a_submitted_file_does_not_become_an_installed_job() {
    let dir = data_dir("uninstalled");
    let file = job_file(&dir, "hello.yaml", HELLO);

    let out = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy()]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    let listed = flowlite(&dir, &["job", "list"]);
    let stdout = String::from_utf8_lossy(&listed.stdout).to_string();

    assert!(!stdout.contains("hello"), "the file was installed: {stdout}");
}

/// `job_id` is `mem.job`'s primary key, so the alternative to refusing is a raw constraint
/// error. The message has to name the command that does what they meant.
#[test]
fn a_file_whose_id_is_already_installed_is_refused() {
    let dir = data_dir("collision");
    install_job(&dir, "hello.yaml", HELLO);
    let file = job_file(&dir, "other.yaml", HELLO);

    let out = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy()]);
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    assert!(!out.status.success(), "the collision was accepted");
    assert!(stderr.contains("hello"), "{stderr}");
    assert!(stderr.contains("job submit hello"), "{stderr}");
}

/// The same validators an installed job is held to, reaching a file that was never
/// installed — and naming the file, not a job in the data directory.
#[test]
fn a_file_whose_tasks_cycle_is_refused_naming_the_file() {
    let dir = data_dir("cycle");
    let file = job_file(
        &dir,
        "cycle.yaml",
        "id: loop\nname: Loop\ntasks:\n\
         \x20 - id: a\n    description: A\n    command: \"true\"\n    depends_on: [b]\n\
         \x20 - id: b\n    description: B\n    command: \"true\"\n    depends_on: [a]\n",
    );

    let out = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy()]);
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    assert!(!out.status.success(), "the cycle was accepted");
    assert!(stderr.contains("cycle.yaml"), "{stderr}");
}

/// Without this the check would only happen in `serve`, which never sees this job — so an
/// unconfigured secret would degrade from a refusal into a spawn-time failure.
#[test]
fn a_file_naming_an_undefined_secret_is_refused() {
    let dir = data_dir("secret");
    let file = job_file(
        &dir,
        "needs-secret.yaml",
        "id: needs-secret\nname: Needs a secret\ntasks:\n\
         \x20 - id: load\n    description: Loads\n    command: \"true\"\n    \
         secret_env:\n      PGPASSWORD: warehouse_pw\n",
    );

    let out = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy()]);
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    assert!(!out.status.success(), "the undefined secret was accepted");
    assert!(stderr.contains("warehouse_pw"), "{stderr}");
}

/// The scope of that check. By the time the file is seeded, `mem` also holds every
/// installed job, so an unscoped check would refuse this submit over a job it has nothing
/// to do with.
#[test]
fn an_installed_job_with_an_undefined_secret_does_not_refuse_a_file_submit() {
    let dir = data_dir("secret-scope");
    install_job(
        &dir,
        "nightly.yaml",
        "id: nightly\nname: Nightly\ntasks:\n\
         \x20 - id: load\n    description: Loads\n    command: \"true\"\n    \
         secret_env:\n      PGPASSWORD: warehouse_pw\n",
    );
    let file = job_file(&dir, "hello.yaml", HELLO);

    let out = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy()]);

    assert!(
        out.status.success(),
        "refused over an unrelated job: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

/// End to end: the run is ordinary once written, so a served directory runs it and
/// `--wait` reports how it ended.
#[test]
fn a_served_directory_runs_a_file_submit_to_success() {
    let dir = data_dir("runs");
    let file = job_file(&dir, "hello.yaml", HELLO);

    let mut server = ServerGuard::new(serve(&dir, 18220), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let out = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy(), "--wait"]);
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();

    server.stop();

    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("succeeded"), "{stdout}");
}

/// The differentiated half: the run carries its own definition, so it outlives the file it
/// came from. Nothing else in the system can answer this once the file is gone.
#[test]
fn a_file_submit_is_still_rerunnable_after_the_file_is_deleted() {
    let dir = data_dir("rerun");
    let file = job_file(&dir, "hello.yaml", HELLO);

    let submitted = flowlite(&dir, &["job", "submit", "-f", &file.to_string_lossy(), "--json"]);
    assert!(submitted.status.success(), "{}", String::from_utf8_lossy(&submitted.stderr));

    let run: serde_json::Value =
        serde_json::from_slice(&submitted.stdout).expect("--json did not print a run");
    let run_id = run["id"].as_i64().unwrap().to_string();

    std::fs::remove_file(&file).unwrap();

    let rerun = flowlite(&dir, &["job-run", "rerun", &run_id]);

    assert!(
        rerun.status.success(),
        "the run could not be replayed without its file: {}",
        String::from_utf8_lossy(&rerun.stderr),
    );
}

/// The dashboard links a run to its job, and an ad-hoc run has none to link to. The id
/// still has to be readable - it is what `job-run list --job` filters on - so it is shown
/// as text rather than as a link into a page that cannot exist.
#[tokio::test]
async fn the_run_page_shows_an_absent_job_as_text_rather_than_a_link() {
    install_crypto_provider();

    let dir = data_dir("run-page");
    let file = job_file(&dir, "hello.yaml", HELLO);

    let mut server = ServerGuard::new(serve(&dir, 18221), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let submitted = flowlite(
        &dir,
        &["job", "submit", "-f", &file.to_string_lossy(), "--wait", "--json"],
    );
    let run: serde_json::Value = serde_json::from_slice(&submitted.stdout).unwrap();
    let run_id = run["id"].as_i64().unwrap();

    let page = reqwest::get(format!("http://127.0.0.1:18221/job-runs/{run_id}"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    server.stop();

    assert!(page.contains("hello"), "the job id is not on the page at all");
    assert!(
        !page.contains("href=\"/jobs/hello\""),
        "the run still links to a job that does not exist",
    );
}

/// The other half of the same rough edge, reachable today by deleting a job that has runs:
/// the job page has to be a page, not the bare string it renders now.
#[tokio::test]
async fn the_page_for_an_absent_job_is_a_real_page() {
    install_crypto_provider();

    let dir = data_dir("job-page");

    let mut server = ServerGuard::new(serve(&dir, 18222), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    let page = reqwest::get("http://127.0.0.1:18222/jobs/gone")
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    server.stop();

    assert_ne!(page.trim(), "Job not found");
    assert!(page.contains("<nav"), "the page is missing the site's own layout");
    assert!(page.contains("gone"), "the page does not say which job is missing");
}
