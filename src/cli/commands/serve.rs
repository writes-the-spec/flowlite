use clap::Args;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::router::app::app::create_router;
use crate::router::app::app_state::AppState;
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::orchestrator::Orchestrator;
use crate::poller::{Poller, POLL_INTERVAL};
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
            POLL_INTERVAL,
        ).start();

        let orchestrator = Orchestrator::new(
            crud.clone(),
            conn_pool.clone(),
            signals.clone(),
        );

        orchestrator.start();

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
        axum::serve(listener, router).await?;

        Ok(())
    }

}