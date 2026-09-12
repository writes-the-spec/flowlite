use sqlx::{SqliteConnection, SqlitePool, Connection};
use crate::app_config::AppConfig;
use std::path::{Path};
use chrono::{DateTime, Utc};
use argon2::{
    password_hash::{
        PasswordHash, PasswordHasher, PasswordVerifier, SaltString
    },
    Argon2
};
use sha2::{Sha256, Digest};
use rand::rngs::OsRng;
use sqlx::sqlite::SqlitePoolOptions;
use uuid::Uuid;


#[derive(Clone)]
pub struct Toolkit {
    pub app_config: AppConfig,
    /// The shared-cache name `mem` is attached under. One process, one name: two
    /// `Toolkit`s sharing a name would seed the same `mem.job` rows into each other, which
    /// is exactly what `with_fresh_mem` exists to avoid.
    pub mem_name: String,
}

/// Serializes every schema migration this process runs, disk or memory.
///
/// Discovered while building the MCP `submit_job` tool's concurrency test (the regression
/// test for `with_fresh_mem` itself, in `src/mcp/tools.rs`). `with_fresh_mem` gives each
/// call a `mem` name nothing else has, so on that path no two concurrent calls share a
/// cache and the one object they still share is the `flowlite.db` *file* - which is what
/// races there. `serve` is the other path and shares more: its pool attaches one
/// `cache=shared` `mem` for the whole process, so two of its connections genuinely do
/// share a cache, and migrating one database while a racing connection holds the other
/// open is where `SQLITE_LOCKED_SHAREDCACHE` ("database schema is locked") was observed.
///
/// sqlx-sqlite implements `Migrate::lock` and `unlock` as no-ops, so nothing serializes two
/// connections migrating the same fresh database: both read zero applied migrations and
/// both run the same `CREATE TABLE`s. A CLI command never hit this because it is the only
/// connection its process ever opens; `serve` and `mcp` are the first long-lived processes
/// where a second migration can start before the first has finished.
///
/// This is not free even once the schema is current: `ensure_migrations_table` issues a
/// `CREATE TABLE IF NOT EXISTS` - a write, taking a write lock - on every connection open,
/// so every migration here is a write and not just a read of `_sqlx_migrations`.
///
/// It is the in-process analogue of the `ServeLock` (`src/cli/commands/serve.rs:37`), taken
/// cross-process for the same reason, and it does **not** close the cross-process case: two
/// flowlite processes first-migrating one fresh data directory can still collide, since
/// only `serve` holds that lock.
static MIGRATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());


impl Toolkit {

    pub fn new(app_config: AppConfig) -> Self {
        Self { app_config, mem_name: "flowlite_mem".to_string() }
    }

    /// The same toolkit, attaching a `mem` nothing else has the name of - for a process
    /// that seeds it more than once, where the shared default name would make the second
    /// seed a primary-key collision on the first's rows instead of a private view.
    pub fn with_fresh_mem(&self) -> Toolkit {
        Toolkit {
            mem_name: format!("flowlite_mem_{}", Uuid::new_v4()),
            ..self.clone()
        }
    }

    pub async fn get_conn_pool(&self) -> anyhow::Result<SqlitePool> {
        let connection_string = self.create_db_if_not_exists().await?;
        let mem_name = self.mem_name.clone();
        let conn_pool = SqlitePoolOptions::new()
            .after_connect(move |conn, _meta| {
                let attach = format!(
                    "ATTACH DATABASE 'file:{}?mode=memory&cache=shared' AS mem",
                    mem_name,
                );
                Box::pin(async move {
                    sqlx::query(sqlx::AssertSqlSafe(attach))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&connection_string)
            .await?;

        let mut conn = conn_pool.acquire().await?;
        self.update_disk_schema(&mut conn).await?;

        Ok(conn_pool)
    }

    pub async fn get_conn(&self) -> anyhow::Result<SqliteConnection> {
        let connection_string = self.create_db_if_not_exists().await?;
        let mut conn = SqliteConnection::connect(&connection_string).await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ATTACH DATABASE 'file:{}?mode=memory&cache=shared' AS mem",
            self.mem_name,
        )))
            .execute(&mut conn)
            .await?;
        self.update_disk_schema(&mut conn).await?;
        Ok(conn)
    }

    pub async fn get_memory_conn(&self) -> anyhow::Result<SqliteConnection> {
        let mut conn = SqliteConnection::connect(&format!(
            "sqlite:file:{}?mode=memory&cache=shared",
            self.mem_name,
        )).await?;
        self.update_memory_schema(&mut conn).await?;
        Ok(conn)
    }

    async fn create_db_if_not_exists(&self) -> anyhow::Result<String> {
        let data_dir = Path::new(&self.app_config.data_dir);
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir)?;
        }
        let db_path = data_dir.join("flowlite.db");
        let connection_string = format!("sqlite:{}?mode=rwc", db_path.to_string_lossy());

        Ok(connection_string)
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
    /// Serialized on `MIGRATION_LOCK` - see that constant for why.
    async fn update_disk_schema(&self, conn: &mut SqliteConnection) -> anyhow::Result<()> {
        let _guard = MIGRATION_LOCK.lock().await;
        sqlx::migrate!("./db/schemas/disk/migrations").run_direct(None, conn, false).await?;
        Ok(())
    }

    /// Concrete for the same reason as `update_disk_schema`: `Migrator::run_direct` over a
    /// plain connection, sidestepping `.run()`'s `Acquire<'e>` bound entirely rather than
    /// hitting the "not general enough" failure inside a caller that must itself be `Send`
    /// for a lifetime this fn cannot see (an rmcp `#[tool]` fn's boxed future).
    ///
    /// Serialized on `MIGRATION_LOCK` too, for the same reason `update_disk_schema` is -
    /// this one is safe on its own today, since `with_fresh_mem` gives every MCP call a
    /// name nothing else shares, but a shared lock for every migration is one invariant to
    /// hold rather than a rule with an exception for the schema that happens to be safe now.
    pub async fn update_memory_schema(&self, conn: &mut SqliteConnection) -> anyhow::Result<()> {
        let _guard = MIGRATION_LOCK.lock().await;
        sqlx::migrate!("./db/schemas/memory/migrations").run_direct(None, conn, false).await?;

        Ok(())
    }

    pub fn argon2_hash(&self, password: &str) -> anyhow::Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let password_hash = argon2.hash_password(password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!("failed to hash password: {}", e))?
            .to_string();
        Ok(password_hash)
    }

    pub fn argon2_verify(&self, password: &str, hash: &str) -> anyhow::Result<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| anyhow::anyhow!("failed to parse password hash: {}", e))?;
        let argon2 = Argon2::default();
        match argon2.verify_password(password.as_bytes(), &parsed_hash) {
            Ok(_) => Ok(true),
            Err(argon2::password_hash::Error::Password) => Ok(false),
            Err(e) => Err(anyhow::anyhow!("failed to verify password: {}", e)),
        }
    }

    pub fn sha256_hash(&self, data: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let result = hasher.finalize();
        format!("{:x}", result)
    }

    pub fn generate_token(&self) -> String {
        Uuid::new_v4().to_string().replace("-", "")
    }

    pub fn get_current_ts(&self) -> DateTime<Utc> {
        Utc::now()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::crud::CRUD;
    use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};

    /// Writes one job file into a fresh temp data dir, for a toolkit of its own to seed
    /// through `CRUD::init` - the faithful path into `mem.job`, rather than an insert
    /// built by hand.
    fn data_dir_with_job(job_id: &str) -> std::path::PathBuf {
        let data_dir = std::env::temp_dir().join(format!("flowlite-toolkit-test-{}", Uuid::new_v4()));
        let jobs_dir = data_dir.join("jobs");
        std::fs::create_dir_all(&jobs_dir).unwrap();
        std::fs::write(jobs_dir.join("job.yaml"), format!("
id: {job_id}
name: Job
tasks:
  - id: only
    command: ./run.sh
")).unwrap();
        data_dir
    }

    /// Two `with_fresh_mem` toolkits seeding the *same* job id are each other's whole
    /// reason to exist: sharing `flowlite_mem` would make the second seed a primary-key
    /// collision on the first's `mem.job` row (`src/test_support.rs:71`), and a long-lived
    /// MCP server seeding more than once in a process is exactly that collision waiting to
    /// happen. A private name per toolkit turns the collision into two untouched views.
    #[tokio::test]
    async fn two_fresh_mem_toolkits_are_invisible_to_each_other() {
        let data_dir_a = data_dir_with_job("nightly-sync");
        let data_dir_b = data_dir_with_job("nightly-sync");

        let toolkit_a = Toolkit::new(AppConfig {
            data_dir: data_dir_a.to_string_lossy().into_owned(),
            ..AppConfig::default()
        }).with_fresh_mem();

        let toolkit_b = Toolkit::new(AppConfig {
            data_dir: data_dir_b.to_string_lossy().into_owned(),
            ..AppConfig::default()
        }).with_fresh_mem();

        assert_ne!(toolkit_a.mem_name, toolkit_b.mem_name);

        // Kept alive for the test's duration: a `mode=memory` database is dropped the
        // instant nothing has it open, and this is the connection that migrated it.
        let mut mem_conn_a = SqliteConnection::connect(&format!(
            "file:{}?mode=memory&cache=shared", toolkit_a.mem_name,
        )).await.unwrap();
        toolkit_a.update_memory_schema(&mut mem_conn_a).await.unwrap();

        let mut mem_conn_b = SqliteConnection::connect(&format!(
            "file:{}?mode=memory&cache=shared", toolkit_b.mem_name,
        )).await.unwrap();
        toolkit_b.update_memory_schema(&mut mem_conn_b).await.unwrap();

        let conn_pool_a = toolkit_a.get_conn_pool().await.unwrap();
        let conn_pool_b = toolkit_b.get_conn_pool().await.unwrap();

        let crud_a = CRUD::new(Arc::new(toolkit_a));
        let crud_b = CRUD::new(Arc::new(toolkit_b));

        let mut conn_a = conn_pool_a.acquire().await.unwrap();
        crud_a.init(&mut conn_a).await.unwrap();

        let mut conn_b = conn_pool_b.acquire().await.unwrap();
        crud_b.init(&mut conn_b).await.unwrap();

        let jobs_a = crud_a.select_jobs(&conn_pool_a, &SelectJobsData {
            filter: SelectJobsDataFilter { job_id: None, name_like: None },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap();

        let jobs_b = crud_b.select_jobs(&conn_pool_b, &SelectJobsData {
            filter: SelectJobsDataFilter { job_id: None, name_like: None },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap();

        assert_eq!(jobs_a.len(), 1);
        assert_eq!(jobs_b.len(), 1);

        let _ = std::fs::remove_dir_all(&data_dir_a);
        let _ = std::fs::remove_dir_all(&data_dir_b);
    }

    /// `Toolkit::new` must still open `flowlite_mem` - every existing CLI command and test
    /// relies on that name unchanged. Asserted on the field alone, with nothing seeded
    /// into it: Cargo runs a binary's tests as threads of one process, so a test that
    /// seeds `flowlite_mem` would race every other test's connection over that one shared
    /// name, which is what `src/test_support.rs:71` and `crud_with_private_mem`'s doc
    /// comment (`src/crud/crud.rs:774`) both warn against.
    #[test]
    fn new_toolkit_opens_the_shared_flowlite_mem() {
        let toolkit = Toolkit::new(AppConfig::default());

        assert_eq!(toolkit.mem_name, "flowlite_mem");
    }
}
