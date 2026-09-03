use clap::Args;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::router::app::app::create_router;
use crate::router::app::app_state::AppState;
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::orchestrator::Orchestrator;
use crate::scheduler::Scheduler;

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

        let scheduler = Scheduler::new(
            toolkit.clone(),
            crud.clone(),
            conn_pool.clone(),
        );

        scheduler.start();

        let orchestrator = Orchestrator::new(
            crud.clone(),
            conn_pool.clone(),
        );

        orchestrator.start();

        let app_state = AppState::new(toolkit.clone(), conn_pool.clone(), memory_conn.clone());

        let router = create_router(app_state);

        let addr: SocketAddr = format!("{}:{}", self.address, self.port).parse()?;
        println!("Listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, router).await?;

        Ok(())
    }

}