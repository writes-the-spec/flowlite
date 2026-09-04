use std::sync::Arc;
use sqlx::{SqlitePool, SqliteConnection};
use crate::signals::Signals;
use crate::toolkit::Toolkit;
use tokio::sync::Mutex;


#[derive(Clone)]
pub struct AppState {
    pub toolkit: Arc<Toolkit>,
    pub conn_pool: Arc<SqlitePool>,
    pub signals: Arc<Signals>,
    memory_conn: Arc<Mutex<SqliteConnection>>,
}


impl AppState {

    pub fn new(toolkit: Arc<Toolkit>, conn_pool: Arc<SqlitePool>, memory_conn: Arc<Mutex<SqliteConnection>>, signals: Arc<Signals>) -> Self {
        Self {
            toolkit,
            conn_pool,
            signals,
            memory_conn,
        }
    }

}