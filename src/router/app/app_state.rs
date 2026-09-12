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
    /// Never read, and held for exactly that reason: a `mode=memory` SQLite database
    /// exists only while something has it open, so dropping this would take `mem`'s schema
    /// and every seeded job with it while the pool went on attaching an empty one.
    #[allow(dead_code)]
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