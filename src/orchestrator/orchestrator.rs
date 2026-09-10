use std::sync::Arc;
use crate::crud::CRUD;
use crate::orchestrator::recovery::{recover_orphaned_task_run_attempts, system_boot_time};
use crate::orchestrator::job_run_dispatcher::JobRunDispatcher;
use crate::orchestrator::job_run_monitor::JobRunMonitor;
use crate::orchestrator::task_run_attempt_children::TaskRunAttemptChildren;
use crate::orchestrator::task_run_attempt_dispatcher::TaskRunAttemptDispatcher;
use crate::orchestrator::task_run_attempt_monitor::TaskRunAttemptMonitor;
use crate::orchestrator::task_run_dispatcher::TaskRunDispatcher;
use crate::orchestrator::task_run_monitor::TaskRunMonitor;
use crate::app_config::AppConfig;
use crate::poller::Poller;
use crate::signals::Signals;


/// Starts the background services that turn job runs into finished task runs. They
/// coordinate through the database only, so this is the one place that has to know all
/// of them, and the order they are started in doesn't matter.
pub struct Orchestrator {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
    /// The child processes the two attempt services share: the dispatcher spawns them,
    /// the monitor waits on them, and `shutdown` kills whatever is left.
    pub children: Arc<TaskRunAttemptChildren>,
    /// config.toml, or its defaults.
    pub app_config: AppConfig,
}


impl Orchestrator {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        signals: Arc<Signals>,
        app_config: AppConfig,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            signals,
            children: Arc::new(TaskRunAttemptChildren::new()),
            app_config,
        }
    }

    /// Kills every task still running. Called once, as `serve` returns.
    ///
    /// Each attempt is in its own process group, so nothing kills them for us: before
    /// they were grouped, Ctrl-C reached them only because they shared this process's
    /// foreground group.
    pub async fn shutdown(self: &Self) {
        self.children.kill_all().await;
    }

    /// Settles what an earlier run of the program left mid-flight, and kills the processes
    /// it can prove are still its own.
    ///
    /// Must be awaited **before** `start`: `TaskRunAttemptChildren` is empty until a
    /// dispatcher fills it, so every Running attempt here belongs to a process this run
    /// does not hold — but once the pollers are going, `TaskRunAttemptMonitor` settles
    /// those rows without ever reading the group id, and their commands keep running.
    pub async fn recover(&self) -> anyhow::Result<()> {
        recover_orphaned_task_run_attempts(
            &self.crud,
            &self.conn_pool,
            system_boot_time(),
        ).await
    }

    /// Spawns every service and returns immediately.
    ///
    /// Every wake-up is registered before any Poller is spawned, so the first service's
    /// own startup pass can never publish to a signal the others haven't registered yet.
    pub fn start(self: &Self) {

        let job_run_dispatcher_wakeup = self.signals.register();
        let job_run_monitor_wakeup = self.signals.register();
        let task_run_dispatcher_wakeup = self.signals.register();
        let task_run_monitor_wakeup = self.signals.register();
        let task_run_attempt_dispatcher_wakeup = self.signals.register();
        let task_run_attempt_monitor_wakeup = self.signals.register();

        let job_run_dispatcher = JobRunDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        );

        let job_run_monitor = JobRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        );

        let task_run_dispatcher = TaskRunDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        );

        let task_run_monitor = TaskRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        );

        let task_run_attempt_dispatcher = TaskRunAttemptDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.children.clone(),
            self.signals.clone(),
            self.app_config.clone(),
        );

        let task_run_attempt_monitor = TaskRunAttemptMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.children.clone(),
            self.signals.clone(),
            self.app_config.clone(),
        );

        Poller::new(Arc::new(job_run_dispatcher), job_run_dispatcher_wakeup, self.app_config.clone()).start();
        Poller::new(Arc::new(job_run_monitor), job_run_monitor_wakeup, self.app_config.clone()).start();
        Poller::new(Arc::new(task_run_dispatcher), task_run_dispatcher_wakeup, self.app_config.clone()).start();
        Poller::new(Arc::new(task_run_monitor), task_run_monitor_wakeup, self.app_config.clone()).start();
        Poller::new(Arc::new(task_run_attempt_dispatcher), task_run_attempt_dispatcher_wakeup, self.app_config.clone()).start();
        Poller::new(Arc::new(task_run_attempt_monitor), task_run_attempt_monitor_wakeup, self.app_config.clone()).start();

    }

}
