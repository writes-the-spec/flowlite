use clap::Args;
use std::net::SocketAddr;
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

#[derive(Args)]
pub struct ServeCmd {
    #[arg(long, default_value = "127.0.0.1")]
    pub address: String,

    #[arg(long, default_value_t = 8000)]
    pub port: u16,
}


impl ServeCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let toolkit = Arc::new(toolkit);

        let memory_conn = toolkit.get_memory_conn().await?;
        let memory_conn = Arc::new(Mutex::new(memory_conn));

        let crud = CRUD::new(toolkit.clone());
        let crud = Arc::new(crud);

        let conn_pool = toolkit.get_conn_pool().await?;
        let conn_pool = Arc::new(conn_pool);

        crud.init(&*conn_pool).await?;

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
        println!("Listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
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