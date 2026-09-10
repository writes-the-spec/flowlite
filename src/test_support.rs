use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use axum::Json;
use axum::extract::State;
use axum::routing::post;
use chrono::{DateTime, Utc};
use crate::app_config::{AppConfig, AppConfigSlack};
use crate::crud::CRUD;
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput};
use crate::crud::job_run_notification::{InsertJobRunNotificationData, InsertJobRunNotificationDataInput, JobRunNotification, JobRunNotificationStatus, NotificationChannel, NotifyOn, SelectJobRunNotificationsData, SelectJobRunNotificationsDataFilter, SelectJobRunNotificationsDataSort};
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRun, TaskRunStatus};
use crate::crud::task_run_attempt::{InsertTaskRunAttemptData, InsertTaskRunAttemptDataInput, SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use crate::orchestrator::job_run_dispatcher::JobRunDispatcher;
use crate::orchestrator::job_run_monitor::JobRunMonitor;
use crate::notifications::NotificationService;
use crate::notifications::channel::NotificationChannels;
use crate::crud::task_run_attempt_output::{group_task_run_attempt_output, SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, SelectTaskRunAttemptOutputsDataSort, TaskRunAttemptOutputStream, TaskRunAttemptOutputStreams};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::orchestrator::task_run_attempt_reader::read_task_run_attempt_stream;
use crate::orchestrator::task_run_attempt_dispatcher::TaskRunAttemptDispatcher;
use crate::orchestrator::task_run_attempt_monitor::TaskRunAttemptMonitor;
use crate::orchestrator::task_run_dispatcher::TaskRunDispatcher;
use crate::orchestrator::task_run_monitor::TaskRunMonitor;
use crate::poller::Service;
use crate::signals::Signals;
use crate::toolkit::Toolkit;


/// The process environment, which no test owns alone.
///
/// `AppConfig::load` reads it, and a task run attempt's spawn now reads it too - the child
/// must not inherit flowlite's own `FLOWLITE_*` configuration. Cargo runs the tests of one
/// binary as threads of one process, so a test that sets a `FLOWLITE_` variable sets it for
/// every load and every spawn running beside it. In edition 2024 that is not merely a
/// logical race: `std::env::set_var` is unsafe because it is undefined behaviour beside a
/// concurrent `std::env::vars()`.
///
/// One writer and many readers is the shape of the problem exactly, so:
///
/// - a test that **sets** a variable takes `writing_the_environment()`, for as long as it
///   is set;
/// - a test that **reads** the environment - loading a config, or spawning a command -
///   takes `reading_the_environment()`.
///
/// A lock rather than a convention, because the failure it prevents is a test that passes
/// alone and fails in a full run, blaming whichever load or spawn happened to overlap.
static ENVIRONMENT: RwLock<()> = RwLock::new(());

/// Taken by every test that loads a config or spawns a command. Bind it to a name - a
/// `let _` drops the guard on the spot and holds nothing.
pub fn reading_the_environment() -> RwLockReadGuard<'static, ()> {
    ENVIRONMENT.read().unwrap_or_else(PoisonError::into_inner)
}

/// Taken by a test that sets a variable, for as long as it is set.
pub fn writing_the_environment() -> RwLockWriteGuard<'static, ()> {
    ENVIRONMENT.write().unwrap_or_else(PoisonError::into_inner)
}


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
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..AppConfig::default()
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

    pub fn job_run_dispatcher(&self) -> JobRunDispatcher {
        JobRunDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        )
    }

    pub fn job_run_monitor(&self) -> JobRunMonitor {
        JobRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        )
    }

    /// A notification service over this test's config, which configures no channel at
    /// all — so it is the service a box with neither `[smtp]` nor `[slack]` runs.
    pub fn notification_service(&self) -> NotificationService {
        NotificationService::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            Arc::new(NotificationChannels::from_config(&self.app_config())),
        )
    }

    /// The same service over a config whose `[slack]` is a fake Slack the test is
    /// running, which is what makes the delivered path assertable — the message a real run
    /// builds, the post it becomes, and the `sent` recorded on the row afterwards — with
    /// no workspace anywhere near it.
    pub fn notification_service_with_slack(&self, slack: &FakeSlack) -> NotificationService {

        let app_config = AppConfig {
            slack: Some(slack.config()),
            ..self.app_config()
        };

        NotificationService::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            Arc::new(NotificationChannels::from_config(&app_config)),
        )
    }

    pub fn task_run_dispatcher(&self) -> TaskRunDispatcher {
        TaskRunDispatcher::new(
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
            self.app_config(),
        )
    }

    /// The same dispatcher, over a config carrying the given secrets - `app_config()`
    /// otherwise always answers an empty map, since nothing under test configures a
    /// `[secrets]` section of its own. Follows `notification_service_with_slack`'s
    /// pattern: the one field a test needs is overridden on top of the real config.
    pub fn task_run_attempt_dispatcher_with_secrets(&self, secrets: BTreeMap<String, String>) -> TaskRunAttemptDispatcher {
        TaskRunAttemptDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.children.clone(),
            self.signals.clone(),
            AppConfig {
                secrets,
                ..self.app_config()
            },
        )
    }

    pub fn task_run_attempt_monitor(&self) -> TaskRunAttemptMonitor {
        TaskRunAttemptMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.children.clone(),
            self.signals.clone(),
            self.app_config(),
        )
    }

    /// The config the CRUD under test is actually using, rather than a fresh default: a
    /// service built here has to agree with `data_dir()` about which directory it is
    /// serving, since that directory is what a task command is told to work on.
    pub fn app_config(&self) -> AppConfig {
        self.crud.toolkit.app_config.clone()
    }

    pub async fn insert_job_run(&self, status: JobRunStatus) -> JobRun {

        let id = self.crud.insert_job_run(
            &*self.conn_pool,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: "job".to_string(),
                    job_name: "Job".to_string(),
                    job_description: String::new(),
                    parameters: BTreeMap::new(),
                    scheduled_at: None,
                    status,
                },
            },
        ).await.unwrap();

        self.job_run(id).await
    }

    /// A job run carrying parameters, for the tests that follow one to a spawned command.
    pub async fn insert_job_run_with_parameters(
        &self,
        status: JobRunStatus,
        parameters: BTreeMap<String, String>,
        scheduled_at: Option<DateTime<Utc>>,
    ) -> JobRun {

        let id = self.crud.insert_job_run(
            &*self.conn_pool,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: "job".to_string(),
                    job_name: "Job".to_string(),
                    job_description: String::new(),
                    parameters,
                    scheduled_at,
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
                    env: BTreeMap::new(),
                    secret_env: BTreeMap::new(),
                    working_dir: String::new(),
                    status: TaskRunStatus::Running,
                },
            },
        ).await.unwrap();

        self.task_run(id).await
    }

    /// A task run carrying a command plus the environment and cwd it should run with.
    pub async fn insert_task_run_for_command_with_env(
        &self,
        job_run_id: i64,
        command: &str,
        env: BTreeMap<String, String>,
        working_dir: &str,
    ) -> TaskRun {

        let id = self.crud.insert_task_run(
            &*self.conn_pool,
            &InsertTaskRunData {
                input: InsertTaskRunDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    task_id: format!("task-{}", uuid::Uuid::new_v4()),
                    command: command.to_string(),
                    depends_on: Vec::new(),
                    timeout: 3600,
                    max_retries: 0,
                    retry_delay: 60,
                    env,
                    secret_env: BTreeMap::new(),
                    working_dir: working_dir.to_string(),
                    status: TaskRunStatus::Running,
                },
            },
        ).await.unwrap();

        self.task_run(id).await
    }

    /// A task run carrying a command plus a `secret_env` mapping, for the leak regression -
    /// separate from `insert_task_run_for_command_with_env` because that helper's env is
    /// always literals, and the two must never be conflated with a stored secret name.
    pub async fn insert_task_run_for_command_with_secret_env(
        &self,
        job_run_id: i64,
        command: &str,
        secret_env: BTreeMap<String, String>,
    ) -> TaskRun {

        let id = self.crud.insert_task_run(
            &*self.conn_pool,
            &InsertTaskRunData {
                input: InsertTaskRunDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    task_id: format!("task-{}", uuid::Uuid::new_v4()),
                    command: command.to_string(),
                    depends_on: Vec::new(),
                    timeout: 3600,
                    max_retries: 0,
                    retry_delay: 60,
                    env: BTreeMap::new(),
                    secret_env,
                    working_dir: String::new(),
                    status: TaskRunStatus::Running,
                },
            },
        ).await.unwrap();

        self.task_run(id).await
    }

    /// A pending task run waiting on the named task ids, which need not have rows - a
    /// dependency with none is one of the states the dispatcher has to settle.
    pub async fn insert_task_run_depending_on(&self, job_run_id: i64, depends_on: &[&str]) -> TaskRun {

        let id = self.crud.insert_task_run(
            &*self.conn_pool,
            &InsertTaskRunData {
                input: InsertTaskRunDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    task_id: format!("task-{}", uuid::Uuid::new_v4()),
                    command: "true".to_string(),
                    depends_on: depends_on.iter().map(|id| id.to_string()).collect(),
                    timeout: 3600,
                    max_retries: 0,
                    retry_delay: 60,
                    env: BTreeMap::new(),
                    secret_env: BTreeMap::new(),
                    working_dir: String::new(),
                    status: TaskRunStatus::Pending,
                },
            },
        ).await.unwrap();

        self.task_run(id).await
    }

    /// A task run under a given task id, so another can be made to depend on it by name.
    pub async fn insert_named_task_run(&self, job_run_id: i64, task_id: &str, status: TaskRunStatus) -> TaskRun {

        let id = self.crud.insert_task_run(
            &*self.conn_pool,
            &InsertTaskRunData {
                input: InsertTaskRunDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    task_id: task_id.to_string(),
                    command: "true".to_string(),
                    depends_on: Vec::new(),
                    timeout: 3600,
                    max_retries: 0,
                    retry_delay: 60,
                    env: BTreeMap::new(),
                    secret_env: BTreeMap::new(),
                    working_dir: String::new(),
                    status,
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
                    env: BTreeMap::new(),
                    secret_env: BTreeMap::new(),
                    working_dir: String::new(),
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

    /// Plants the process group and start instant a previous run of the program would have
    /// left on a Running attempt, which is the state a crash leaves behind.
    pub async fn orphan_task_run_attempt(
        &self,
        task_run_attempt_id: i64,
        process_group_id: i64,
        started_at: DateTime<Utc>,
    ) {
        sqlx::query("UPDATE task_run_attempt SET process_group_id = ?, started_at = ? WHERE id = ?")
            .bind(process_group_id)
            .bind(started_at)
            .bind(task_run_attempt_id)
            .execute(&*self.conn_pool)
            .await
            .unwrap();
    }

    /// Marks a pending attempt as one a spawn was begun for, which is what a crash between
    /// the spawn and the Running write leaves behind. No CRUD update writes `started_at`
    /// without a status, so this reaches past CRUD for a state only a crash produces.
    pub async fn begin_spawn_of_task_run_attempt(&self, task_run_attempt_id: i64) {

        sqlx::query("UPDATE task_run_attempt SET started_at = ? WHERE id = ?")
            .bind(Utc::now())
            .bind(task_run_attempt_id)
            .execute(&*self.conn_pool)
            .await
            .unwrap();
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

    /// An open notification against a run, the way `submit_job` writes one — before
    /// anyone knows whether the run will need it.
    pub async fn insert_job_run_notification(
        &self,
        job_run_id: i64,
        notify_on: NotifyOn,
        channel: NotificationChannel,
        recipients: &[&str],
    ) -> JobRunNotification {

        let id = self.crud.insert_job_run_notification(
            &*self.conn_pool,
            &InsertJobRunNotificationData {
                input: InsertJobRunNotificationDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    notify_on,
                    channel,
                    recipients: recipients.iter().map(|r| r.to_string()).collect(),
                    status: JobRunNotificationStatus::Pending,
                    error: String::new(),
                },
            },
        ).await.unwrap();

        self.job_run_notifications(job_run_id).await
            .into_iter()
            .find(|notification| notification.id == id)
            .unwrap()
    }

    pub async fn job_run_notifications(&self, job_run_id: i64) -> Vec<JobRunNotification> {

        self.crud.select_job_run_notifications(
            &*self.conn_pool,
            &SelectJobRunNotificationsData {
                filter: SelectJobRunNotificationsDataFilter {
                    id: None,
                    job_run_id: Some(job_run_id),
                    notify_on: None,
                    channel: None,
                    status: None,
                },
                sort: Some(SelectJobRunNotificationsDataSort::Id),
                limit: None,
                offset: None,
            },
        ).await.unwrap()
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
                self.app_config(),
            )),
            tokio::spawn(read_task_run_attempt_stream(
                stderr,
                TaskRunAttemptOutputStream::Stderr,
                chunks_sender,
                self.app_config(),
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


/// One request the fake Slack recorded, in the parts a test asks about.
#[derive(Clone)]
pub struct FakeSlackPost {
    pub authorization: String,
    pub channel: String,
    pub text: String,
    pub blocks: serde_json::Value,
}

/// A Slack that answers on localhost, so a send is exercised over the transport it really
/// uses — the header, the JSON body, and the `ok: false` in a 200 — rather than mocked
/// away.
///
/// It lives here rather than in the channel's own tests because the notification service
/// needs one too, and both have to agree with `post_payload` about what a post looks
/// like: one fake to keep up with a change in that shape, not two that can drift.
#[derive(Clone)]
pub struct FakeSlack {
    api_url: String,
    posts: Arc<Mutex<Vec<FakeSlackPost>>>,
    /// The `error` to refuse with, keyed by conversation. Everything else is accepted.
    refusals: Arc<Vec<(String, String)>>,
}


impl FakeSlack {

    pub async fn start() -> Self {
        Self::refusing(&[]).await
    }

    /// The same Slack, refusing the named conversations the way the real one does: an
    /// `error` in a 200 that a send trusting the status code would have called delivered.
    pub async fn refusing(refusals: &[(&str, &str)]) -> Self {

        // Bound before the state is built, since the api_url a caller configures is the
        // address the OS just chose.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let slack = Self {
            api_url: format!("http://{}/chat.postMessage", address),
            posts: Arc::new(Mutex::new(Vec::new())),
            refusals: Arc::new(
                refusals
                    .iter()
                    .map(|(channel, error)| (channel.to_string(), error.to_string()))
                    .collect(),
            ),
        };

        let router = axum::Router::new()
            .route("/chat.postMessage", post(fake_slack_post_message))
            .with_state(slack.clone());

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        slack
    }

    /// The `[slack]` section that reaches this fake, so a test configuring a channel and a
    /// test configuring the whole service ask for it the same way.
    pub fn config(&self) -> AppConfigSlack {
        AppConfigSlack {
            token: "xoxb-test".to_string(),
            api_url: self.api_url.clone(),
            timeout_seconds: 5,
            max_output_bytes: 2048,
        }
    }

    pub fn posts(&self) -> Vec<FakeSlackPost> {
        self.posts.lock().unwrap().clone()
    }

}


async fn fake_slack_post_message(
    State(slack): State<FakeSlack>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {

    let channel = body["channel"].as_str().unwrap().to_string();

    slack.posts.lock().unwrap().push(FakeSlackPost {
        authorization: headers
            .get("authorization")
            .map(|value| value.to_str().unwrap().to_string())
            .unwrap_or_default(),
        channel: channel.clone(),
        text: body["text"].as_str().unwrap().to_string(),
        blocks: body["blocks"].clone(),
    });

    let refusal = slack.refusals
        .iter()
        .find(|(refused, _)| refused == &channel);

    match refusal {
        Some((_, error)) => Json(serde_json::json!({ "ok": false, "error": error })),
        None => Json(serde_json::json!({ "ok": true })),
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

/// Waits for a command to write a file where the test asked it to, and reports its
/// contents — for the tests that ask a command what it saw rather than reading it back
/// through the monitor.
pub async fn read_command_file(path: &std::path::Path) -> String {

    for _ in 0..2000 {
        if let Ok(contents) = std::fs::read_to_string(path)
            && !contents.is_empty()
        {
            return contents.trim().to_string();
        }

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    panic!("nothing was written to {}", path.display());
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
