use clap::Args;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::router::app::app::create_router;
use crate::router::app::app_state::AppState;
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::notifications::NotificationService;
use crate::notifications::channel::NotificationChannels;
use crate::orchestrator::Orchestrator;
use crate::poller::Poller;
use crate::scheduler::Scheduler;
use crate::signals::Signals;
use crate::serve_state::{write_state, ServeLock, ServeState};

#[derive(Args)]
pub struct ServeCmd {
    #[arg(long, default_value = "127.0.0.1")]
    pub address: String,

    #[arg(long, default_value_t = 8000)]
    pub port: u16,
}


impl ServeCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {
        let data_dir = PathBuf::from(&toolkit.app_config.data_dir);

        // Taken before the pool is opened, so a second serve on this directory stops here
        // rather than racing this one through sqlx::migrate! - and, more importantly, so
        // two Schedulers cannot both advance one schedule's next_run and fire every cron
        // twice. Bound to a name so it lives as long as the server: `let _` would release
        // it here.
        let _serve_lock = ServeLock::acquire(&data_dir)?;

        let toolkit = Arc::new(toolkit);

        let memory_conn = toolkit.get_memory_conn().await?;
        let memory_conn = Arc::new(Mutex::new(memory_conn));

        let crud = CRUD::new(toolkit.clone());
        let crud = Arc::new(crud);

        let conn_pool = toolkit.get_conn_pool().await?;
        let conn_pool = Arc::new(conn_pool);

        crud.init(&*conn_pool).await?;

        // Every job's and task's secret_env: must name a secret app_config.secrets
        // actually defines. Checked here, and only here - never inside `init` itself -
        // because `init` also runs inside `job` and `job-run` commands, which seed this
        // very same mem.job/mem.task from this very same YAML in their own process. A
        // check there would mean `flowlite job-run list` refuses to run in any terminal
        // that has not exported the production secrets, and reading a run's status must
        // never require the credentials that run used. This is the one process that will
        // actually spawn a command, so it is the one place this fails before the bind
        // rather than at 03:00.
        crud.check_secret_env_is_satisfied(&*conn_pool, &toolkit.app_config.secrets).await?;

        let signals = Arc::new(Signals::new());

        let app_config = toolkit.app_config.clone();

        // Registered before any Poller is spawned, for the reason Orchestrator::start
        // registers all of its own up front: a poller's first pass runs the moment it is
        // spawned, and must not publish to a wake-up nobody has registered yet.
        let notification_service_wakeup = signals.register();

        let scheduler = Scheduler::new(
            toolkit.clone(),
            crud.clone(),
            conn_pool.clone(),
            signals.clone(),
        );

        // Nothing publishes to this wake-up: the scheduler's work is time-driven, and
        // registering it would wake it on every unrelated status change for nothing.
        Poller::new(
            Arc::new(scheduler),
            Arc::new(tokio::sync::Notify::new()),
            app_config.clone(),
        ).start();

        let orchestrator = Orchestrator::new(
            crud.clone(),
            conn_pool.clone(),
            signals.clone(),
            app_config.clone(),
        );

        // Before the pollers, not after: a Running attempt from an earlier run of the
        // program still names the process group it spawned, and a monitor pass would settle
        // the row without ever reading it.
        orchestrator.recover().await?;

        orchestrator.start();

        // Started alongside the orchestrator rather than inside it, the way the scheduler
        // is: nothing in the orchestrator calls it, and it calls nothing back. It is
        // started whatever config.toml configures — with no channel at all it simply has
        // nothing open to deliver, and says so on the row rather than in a silence.
        let notification_service = NotificationService::new(
            crud.clone(),
            conn_pool.clone(),
            Arc::new(NotificationChannels::from_config(&app_config)),
        );

        Poller::new(
            Arc::new(notification_service),
            notification_service_wakeup,
            app_config.clone(),
        ).start();

        let app_state = AppState::new(
            toolkit.clone(),
            conn_pool.clone(),
            memory_conn.clone(),
            signals.clone(),
        );

        let router = create_router(app_state);

        let addr: SocketAddr = format!("{}:{}", self.address, self.port).parse()?;

        let listener = tokio::net::TcpListener::bind(addr).await?;

        // Off the listener rather than off the flags, so `--port 0` reports the port it
        // was actually given.
        let bound = listener.local_addr()?;

        // Written after the bind, so that a directory reported as served is one whose
        // port is accepting.
        write_state(&data_dir, &ServeState {
            pid: std::process::id(),
            address: bound.ip().to_string(),
            port: bound.port(),
            started_at: chrono::Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?;

        // After the bind too, which it should always have been: it claimed to be
        // listening before it was.
        println!("Listening on http://{}", bound);

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        println!("Stopping running tasks");
        orchestrator.shutdown().await;

        Ok(())
    }

}


/// Resolves on Ctrl-C or SIGTERM, whichever arrives first.
async fn shutdown_signal() {

    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl-C");
    };

    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to listen for SIGTERM")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use crate::app_config::AppConfig;
    use crate::serve_state::ServeLock;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flowlite-serve-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn toolkit_for(data_dir: &Path) -> Toolkit {
        Toolkit::new(AppConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..AppConfig::default()
        })
    }

    /// The lock is taken before the pool is opened, so this fails without ever running a
    /// migration or binding a port - which is what makes it safe to assert on here.
    #[tokio::test]
    async fn a_second_serve_on_one_data_dir_is_refused_naming_the_directory() {
        let dir = temp_dir();

        let _held = ServeLock::acquire(&dir).unwrap();

        let cmd = ServeCmd { address: "127.0.0.1".to_string(), port: 0 };

        let error = cmd.run(toolkit_for(&dir)).await.unwrap_err().to_string();

        assert!(error.contains("already"), "{error}");
        assert!(error.contains(&dir.to_string_lossy().to_string()), "{error}");
    }
}
