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
}


impl Toolkit {
    
    pub fn new(app_config: AppConfig) -> Self {
        Self { app_config }
    }

    pub async fn get_conn_pool(&self) -> anyhow::Result<SqlitePool> {
        let connection_string = self.create_db_if_not_exists().await?;
        let conn_pool = SqlitePoolOptions::new()
            .after_connect(|conn, _meta| Box::pin(async move {
                sqlx::query(
                    "ATTACH DATABASE 'file:flowlite_mem?mode=memory&cache=shared' AS mem"
                )
                    .execute(conn)
                    .await?;
                Ok(())
            }))
            .connect(&connection_string)
            .await?;
        self.update_disk_schema(&conn_pool).await?;
        Ok(conn_pool)
    }

    pub async fn get_conn(&self) -> anyhow::Result<SqliteConnection> {
        let connection_string = self.create_db_if_not_exists().await?;
        let mut conn = SqliteConnection::connect(&connection_string).await?;
        sqlx::query(
            "ATTACH DATABASE 'file:flowlite_mem?mode=memory&cache=shared' AS mem"
        )
            .execute(&mut conn)
            .await?;
        self.update_disk_schema(&mut conn).await?;
        Ok(conn)
    }

    pub async fn get_memory_conn(&self) -> anyhow::Result<SqliteConnection> {
        // let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        let mut conn = SqliteConnection::connect("sqlite:file:flowlite_mem?mode=memory&cache=shared").await?;
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

    async fn update_disk_schema<'e, E>(&self, executor: E) -> anyhow::Result<()>
    where
        E: sqlx::Acquire<'e, Database = sqlx::Sqlite>,
    {
        let mut conn = executor.acquire().await?;
        sqlx::migrate!("./db/schemas/disk/migrations").run(&mut *conn).await?;
        Ok(())
    }

    pub async fn update_memory_schema<'e, E>(&self, executor: E) -> anyhow::Result<()>
    where
        E: sqlx::Acquire<'e, Database = sqlx::Sqlite> + Send,
        <E as sqlx::Acquire<'e>>::Connection: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut conn = executor.acquire().await?;
        sqlx::migrate!("./db/schemas/memory/migrations").run(&mut *conn).await?;

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
