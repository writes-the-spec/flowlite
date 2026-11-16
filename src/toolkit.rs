use sqlx::{SqliteConnection, SqlitePool, Connection};
use crate::app_config::AppConfig;
use crate::serve_state::state_dir;
use std::path::{Path, PathBuf};
use std::time::Duration;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};


#[derive(Clone)]
pub struct Toolkit {
    pub app_config: AppConfig,
}

/// Serializes opening and migrating `flowlite.db` within this process, for two reasons a
/// fresh database has and an established one does not:
///
/// - Switching a database into WAL takes an exclusive lock that `busy_timeout` cannot wait
///   on, so a second connection opening the file mid-switch fails at once with "database
///   is locked". Once the file is in WAL, asking for WAL again changes nothing.
/// - sqlx-sqlite implements `Migrate::lock` and `unlock` as no-ops, so two connections
///   migrating the same fresh database both read zero applied migrations and both run the
///   same `CREATE TABLE`s.
///
/// `serve` and `mcp` are long-lived processes where two connections can open at once - two
/// concurrent MCP tool calls each open their own. In-process only: two flowlite processes
/// opening one fresh project directory can still collide, since only `serve` holds the
/// cross-process `ServeLock`.
static OPEN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// How long a connection waits for another process's write lock on `flowlite.db` before it
/// fails with `SQLITE_BUSY`. `serve`, CLI commands and the MCP server all write the same file.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The database, inside the project directory's state directory rather than beside the YAML:
/// the top of the project directory is the user's own files, and everything flowlite writes -
/// this, its `-wal` and `-shm`, the serve lock and the run directories - sits under one
/// `.flowlite/` that a single `.gitignore` line covers.
const DB_FILE: &str = "flowlite.db";

fn db_path(project_dir: &Path) -> PathBuf {
    state_dir(project_dir).join(DB_FILE)
}


impl Toolkit {

    pub fn new(app_config: AppConfig) -> Self {
        Self { app_config }
    }

    pub async fn get_conn_pool(&self) -> anyhow::Result<SqlitePool> {
        let connect_options = self.disk_connect_options()?;

        let _guard = OPEN_LOCK.lock().await;

        let conn_pool = SqlitePoolOptions::new()
            .connect_with(connect_options)
            .await?;

        let mut conn = conn_pool.acquire().await?;
        self.update_disk_schema(&mut conn).await?;

        Ok(conn_pool)
    }

    pub async fn get_conn(&self) -> anyhow::Result<SqliteConnection> {
        let connect_options = self.disk_connect_options()?;

        let _guard = OPEN_LOCK.lock().await;

        let mut conn = SqliteConnection::connect_with(&connect_options).await?;
        self.update_disk_schema(&mut conn).await?;
        Ok(conn)
    }

    /// Every connection to `flowlite.db` opens with these, so no two connections disagree on
    /// pragmas. WAL lets the pollers, the dashboard and a CLI command read while another
    /// process writes, where the default rollback journal blocks every reader for the length
    /// of each write. `synchronous=NORMAL` is the setting WAL is designed for: a crash of
    /// flowlite loses nothing, a power cut can lose the last commits but never corrupts.
    fn disk_connect_options(&self) -> anyhow::Result<SqliteConnectOptions> {
        let project_dir = Path::new(&self.app_config.project_dir);
        std::fs::create_dir_all(state_dir(project_dir))?;

        let connect_options = SqliteConnectOptions::new()
            .filename(db_path(project_dir))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(BUSY_TIMEOUT);

        Ok(connect_options)
    }

    /// Migrates over a plain connection with `Migrator::run_direct`, sqlx's own documented
    /// escape from `.run()`'s `E: Acquire<'e, ..>` bound - an `async fn` generic over a
    /// single `Acquire<'e>` lifetime cannot be proven `Send` inside a caller that itself
    /// must be `Send` for an unrelated, unnamed lifetime (a boxed `dyn Future` behind a
    /// trait object, which is what an rmcp `#[tool]` fn compiles down to), and rustc
    /// rejects it with "implementation of `sqlx::Acquire` is not general enough" - the
    /// exact failure this repo's own `crud` skill warns about for our own multistatement
    /// methods, here hit one layer down inside sqlx's own `Migrator::run`.
    /// `run_direct(None, conn, false)` is `.run()`'s own body (`sqlx-core`'s
    /// `migrator.rs`), so behavior is unchanged.
    ///
    /// Callers hold `OPEN_LOCK` - see that constant for why.
    async fn update_disk_schema(&self, conn: &mut SqliteConnection) -> anyhow::Result<()> {
        sqlx::migrate!("./db/schemas/disk/migrations").run_direct(None, conn, false).await?;
        Ok(())
    }

    pub fn get_current_ts(&self) -> DateTime<Utc> {
        Utc::now()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// A single connection and a pool connection both carry the pragmas from
    /// `disk_connect_options`, read back from SQLite rather than trusted from the builder.
    #[tokio::test]
    async fn disk_connections_use_wal_normal_sync_and_a_busy_timeout() {
        let project_dir = std::env::temp_dir().join(format!("flowlite-toolkit-test-{}", Uuid::new_v4()));

        let toolkit = Toolkit::new(AppConfig {
            project_dir: project_dir.to_string_lossy().into_owned(),
            ..AppConfig::default()
        });

        let mut conn = toolkit.get_conn().await.unwrap();
        let conn_pool = toolkit.get_conn_pool().await.unwrap();
        let mut pool_conn = conn_pool.acquire().await.unwrap();

        for conn in [&mut conn, &mut *pool_conn] {
            let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            let busy_timeout_ms: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
                .fetch_one(&mut *conn)
                .await
                .unwrap();

            assert_eq!(journal_mode, "wal");
            // 1 is NORMAL; the SQLite default is 2, FULL.
            assert_eq!(synchronous, 1);
            assert_eq!(busy_timeout_ms, 5000);
        }

        let _ = std::fs::remove_dir_all(&project_dir);
    }

    /// The database lands in `.flowlite/`, created along with a project directory that does not
    /// exist yet, and nothing is written at the project directory's top level.
    #[tokio::test]
    async fn the_database_is_created_inside_the_state_directory() {
        let project_dir = std::env::temp_dir().join(format!("flowlite-toolkit-test-{}", Uuid::new_v4()));

        let toolkit = Toolkit::new(AppConfig {
            project_dir: project_dir.to_string_lossy().into_owned(),
            ..AppConfig::default()
        });

        let _conn = toolkit.get_conn().await.unwrap();

        assert!(project_dir.join(".flowlite").join("flowlite.db").exists());
        let top_level: Vec<_> = std::fs::read_dir(&project_dir).unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(top_level, [".flowlite"]);

        let _ = std::fs::remove_dir_all(&project_dir);
    }
}
