use std::path::PathBuf;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::app_config::AppConfig;
use crate::crud::CRUD;
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput};
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRun, TaskRunStatus};
use crate::crud::task_run_attempt::{InsertTaskRunAttemptData, InsertTaskRunAttemptDataInput, SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use crate::orchestrator::job_run_monitor::JobRunMonitor;
use crate::crud::task_run_attempt_output::{group_task_run_attempt_output, SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, SelectTaskRunAttemptOutputsDataSort, TaskRunAttemptOutputStream, TaskRunAttemptOutputStreams};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::orchestrator::task_run_attempt_reader::read_task_run_attempt_stream;
use crate::orchestrator::task_run_attempt_dispatcher::TaskRunAttemptDispatcher;
use crate::orchestrator::task_run_attempt_monitor::TaskRunAttemptMonitor;
use crate::orchestrator::task_run_monitor::TaskRunMonitor;
use crate::poller::Service;
use crate::signals::Signals;
use crate::toolkit::Toolkit;


/// One test's own flowlite database, in a temp directory nothing else shares.
///
/// A service is built here with its real CRUD rather than a fake, so a settle chain is
/// asked what it wrote by reading the row back out of the table the next poll pass would
/// read it from — which is the only channel the services have between them, and the one
/// thing a hand-built struct cannot stand in for.
///
/// The `mem` schema is left unmigrated on purpose: no monitor reads a definition, and the
/// memory database is one shared-cache name for the whole process, so seeding it would
/// leak between tests running in parallel.
pub struct TestDb {
    data_dir: PathBuf,
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
    /// One map for the whole TestDb, the way Orchestrator::start shares it: a test that
    /// spawns a process through the dispatcher can then have the monitor find it.
    pub children: Arc<TaskRunAttemptChildren>,
}


impl TestDb {

    pub async fn new() -> Self {

        let data_dir = std::env::temp_dir().join(format!("flowlite-test-{}", uuid::Uuid::new_v4()));

        let app_config = AppConfig {
            config_dir: data_dir.join("config").to_string_lossy().into_owned(),
            data_dir: data_dir.to_string_lossy().into_owned(),
        };

        let toolkit = Arc::new(Toolkit::new(app_config));

        let conn_pool = toolkit.get_conn_pool().await
            .expect("failed to open the test database");

        Self {
            data_dir,
            crud: Arc::new(CRUD::new(toolkit)),
            conn_pool: Arc::new(conn_pool),
            signals: Arc::new(Signals::new()),
            children: Arc::new(TaskRunAttemptChildren::new()),
        }
    }

    /// The temp directory this test owns, for a command that needs somewhere to write.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    pub fn job_run_monitor(&self) -> JobRunMonitor {
        JobRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        )
    }

    pub fn task_run_monitor(&self) -> TaskRunMonitor {
        TaskRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        )
    }

    pub fn task_run_attempt_dispatcher(&self) -> TaskRunAttemptDispatcher {
        TaskRunAttemptDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.children.clone(),
            self.signals.clone(),
        )
    }

    pub fn task_run_attempt_monitor(&self) -> TaskRunAttemptMonitor {
        TaskRunAttemptMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.children.clone(),
            self.signals.clone(),
        )
    }

    pub async fn insert_job_run(&self, status: JobRunStatus) -> JobRun {

        let id = self.crud.insert_job_run(
            &*self.conn_pool,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: "job".to_string(),
                    job_name: "Job".to_string(),
                    job_description: String::new(),
                    status,
                },
            },
        ).await.unwrap();

        self.job_run(id).await
    }

    /// A task run with nothing to retry, for the tests that only care about its status.
    pub async fn insert_task_run(&self, job_run_id: i64, status: TaskRunStatus) -> TaskRun {
        self.insert_task_run_with(job_run_id, status, 0, 60).await
    }

    /// A running task run with retries left, for the tests that drive the retry loop.
    pub async fn insert_retryable_task_run(&self, job_run_id: i64, max_retries: u32, retry_delay: u32) -> TaskRun {
        self.insert_task_run_with(job_run_id, TaskRunStatus::Running, max_retries, retry_delay).await
    }

    /// A task run carrying a real command, for the tests that let the dispatcher spawn it.
    /// `timeout` of 0 puts the attempt past its deadline the moment it starts.
    pub async fn insert_task_run_for_command(&self, job_run_id: i64, command: &str, timeout: u32) -> TaskRun {

        let id = self.crud.insert_task_run(
            &*self.conn_pool,
            &InsertTaskRunData {
                input: InsertTaskRunDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    task_id: format!("task-{}", uuid::Uuid::new_v4()),
                    command: command.to_string(),
                    depends_on: Vec::new(),
                    timeout,
                    max_retries: 0,
                    retry_delay: 60,
                    status: TaskRunStatus::Running,
                },
            },
        ).await.unwrap();

        self.task_run(id).await
    }

    async fn insert_task_run_with(
        &self,
        job_run_id: i64,
        status: TaskRunStatus,
        max_retries: u32,
        retry_delay: u32,
    ) -> TaskRun {

        let id = self.crud.insert_task_run(
            &*self.conn_pool,
            &InsertTaskRunData {
                input: InsertTaskRunDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    task_id: format!("task-{}", uuid::Uuid::new_v4()),
                    command: "true".to_string(),
                    depends_on: Vec::new(),
                    timeout: 3600,
                    max_retries,
                    retry_delay,
                    status,
                },
            },
        ).await.unwrap();

        self.task_run(id).await
    }

    pub async fn insert_task_run_attempt(
        &self,
        task_run: &TaskRun,
        attempt: u32,
        status: TaskRunAttemptStatus,
    ) -> TaskRunAttempt {

        self.crud.insert_task_run_attempt(
            &*self.conn_pool,
            &InsertTaskRunAttemptData {
                input: InsertTaskRunAttemptDataInput {
                    task_run_id: task_run.id,
                    job_run_id: task_run.job_run_id,
                    job_id: task_run.job_id.clone(),
                    task_id: task_run.task_id.clone(),
                    attempt,
                    status,
                },
            },
        ).await.unwrap();

        self.last_task_run_attempt(task_run.id).await
    }

    /// Moves an attempt's `created_at` back, which is what a retry_delay is measured from.
    /// No CRUD update writes that column — only the insert does — so the test reaches past
    /// CRUD rather than opening a seam in it for a case only a test has.
    pub async fn backdate_task_run_attempt(&self, task_run_attempt_id: i64, created_at: DateTime<Utc>) {

        sqlx::query("UPDATE task_run_attempt SET created_at = ? WHERE id = ?")
            .bind(created_at)
            .bind(task_run_attempt_id)
            .execute(&*self.conn_pool)
            .await
            .unwrap();
    }

    pub async fn job_run(&self, id: i64) -> JobRun {

        self.crud.select_job_run(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: Some(id),
                    job_id: None,
                    status: None,
                },
                sort: None,
                limit: Some(1),
                offset: None,
            },
        ).await.unwrap().unwrap()
    }

    pub async fn task_run(&self, id: i64) -> TaskRun {

        self.crud.select_task_run(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: Some(id),
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            },
        ).await.unwrap().unwrap()
    }

    pub async fn task_run_attempts(&self, task_run_id: i64) -> Vec<TaskRunAttempt> {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: Some(task_run_id),
                    job_run_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Attempt),
            },
        ).await.unwrap()
    }

    async fn last_task_run_attempt(&self, task_run_id: i64) -> TaskRunAttempt {
        self.task_run_attempts(task_run_id).await.into_iter().last().unwrap()
    }

    pub async fn task_run_attempt(&self, task_run_attempt_id: i64) -> TaskRunAttempt {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            },
        ).await.unwrap()
            .into_iter()
            .find(|task_run_attempt| task_run_attempt.id == task_run_attempt_id)
            .unwrap()
    }

    pub async fn insert_job_run_stop(&self, job_run_id: i64) {

        self.crud.insert_job_run_stop(
            &*self.conn_pool,
            &InsertJobRunStopData {
                input: InsertJobRunStopDataInput { job_run_id },
            },
        ).await.unwrap();
    }

    /// Hands the monitor a process that has already exited, reaped here so the test does
    /// not race it: `try_wait` keeps reporting the status once the child has been collected.
    pub async fn spawn_exited_child(
        &self,
        task_run_attempt: &TaskRunAttempt,
        command: &str,
        times_out_at: DateTime<Utc>,
    ) {
        let mut child = Self::spawn(command);

        while child.try_wait().unwrap().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        self.hand_over(task_run_attempt, child, times_out_at).await;
    }

    /// Hands the monitor a process that is still running, with the deadline it is judged
    /// against — in the past for the timeout case.
    pub async fn spawn_running_child(
        &self,
        task_run_attempt: &TaskRunAttempt,
        command: &str,
        times_out_at: DateTime<Utc>,
    ) {
        let child = Self::spawn(command);

        self.hand_over(task_run_attempt, child, times_out_at).await;
    }

    fn spawn(command: &str) -> tokio::process::Child {
        tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }

    /// Hands the child over the way TaskRunAttemptDispatcher does, readers and all — the
    /// original sender moves into the second one rather than being kept here, or the
    /// channel would never close and every terminal pass would wait out its EOF timeout.
    async fn hand_over(
        &self,
        task_run_attempt: &TaskRunAttempt,
        mut child: tokio::process::Child,
        times_out_at: DateTime<Utc>,
    ) {
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let (chunks_sender, chunks) = tokio::sync::mpsc::unbounded_channel();

        let readers = [
            tokio::spawn(read_task_run_attempt_stream(
                stdout,
                TaskRunAttemptOutputStream::Stdout,
                chunks_sender.clone(),
            )),
            tokio::spawn(read_task_run_attempt_stream(
                stderr,
                TaskRunAttemptOutputStream::Stderr,
                chunks_sender,
            )),
        ];

        self.children.insert(
            task_run_attempt.id,
            TaskRunAttemptChild {
                child,
                chunks,
                readers,
                times_out_at,
            },
        ).await;
    }

    /// The output recorded for one attempt, assembled the way the route and the CLI do.
    pub async fn task_run_attempt_output(&self, task_run_attempt_id: i64) -> TaskRunAttemptOutputStreams {

        let rows = self.crud.select_task_run_attempt_outputs(
            &*self.conn_pool,
            &SelectTaskRunAttemptOutputsData {
                filter: SelectTaskRunAttemptOutputsDataFilter {
                    id: None,
                    task_run_attempt_id: Some(task_run_attempt_id),
                    task_run_id: None,
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    stream: None,
                },
                sort: Some(SelectTaskRunAttemptOutputsDataSort::Id),
            },
        ).await.unwrap();

        group_task_run_attempt_output(rows)
            .remove(&task_run_attempt_id)
            .unwrap_or_default()
    }

    /// Runs monitor passes until the attempt has recorded some stdout.
    ///
    /// A reader delivers on its own schedule and `record_output` never waits for one, so a
    /// running attempt's first output lands on some later pass rather than on the pass that
    /// followed the write. Asserting after a single pass is a race.
    pub async fn poll_until_stdout(&self, task_run_attempt: &TaskRunAttempt) -> String {

        for _ in 0..2000 {
            self.task_run_attempt_monitor().handle(task_run_attempt).await.unwrap();

            let stdout = self.task_run_attempt_output(task_run_attempt.id).await.stdout;

            if !stdout.is_empty() {
                return stdout;
            }

            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        panic!("no stdout was recorded for attempt {}", task_run_attempt.id);
    }

}


/// Waits for a command to write a pid where the test asked it to, and reports it.
pub async fn read_pid_file(path: &std::path::Path) -> i32 {

    for _ in 0..2000 {
        if let Ok(contents) = std::fs::read_to_string(path)
            && let Ok(pid) = contents.trim().parse()
        {
            return pid;
        }

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    panic!("no pid was written to {}", path.display());
}

/// Whether the process is gone, waiting up to two seconds for it — a killed grandchild is
/// reparented before it is reaped, so it does not disappear the instant the signal lands.
pub async fn has_exited(pid: i32) -> bool {

    for _ in 0..2000 {
        // Signal 0 checks for the process without sending anything.
        if unsafe { libc::kill(pid, 0) } == -1 {
            return true;
        }

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    false
}


impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}
