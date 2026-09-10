use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskDataInput {
    pub row_id: u64,
    pub task_id: String,
    pub job_id: String,
    pub description: String,
    pub command: String,
    pub depends_on: Vec<String>,
    pub timeout: u32,
    pub max_retries: u32,
    pub retry_delay: u32,
    pub env: BTreeMap<String, String>,
    pub secret_env: BTreeMap<String, String>,
    pub working_dir: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskData {
    pub input: InsertTaskDataInput,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SelectTasksDataSort {
    TaskId,
    RowId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTasksDataFilter {
    pub task_id: Option<String>,
    pub job_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTasksData {
    pub filter: SelectTasksDataFilter,
    pub sort: Option<SelectTasksDataSort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct Task {
    pub task_id: String,
    pub job_id: String,
    pub description: String,
    pub command: String,
    pub depends_on: sqlx::types::Json<Vec<String>>,
    pub timeout: u32,
    pub max_retries: u32,
    pub retry_delay: u32,
    pub env: sqlx::types::Json<BTreeMap<String, String>>,
    /// Environment variable name to secret name - never a value. See `JobYamlTask::secret_env`.
    pub secret_env: sqlx::types::Json<BTreeMap<String, String>>,
    pub working_dir: String,
}


impl CRUD {
    pub async fn insert_task<'e, E>(&self, executor: E, data: &InsertTaskData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO mem.task (row_id, task_id, job_id, description, command, depends_on, timeout, max_retries, retry_delay, env, secret_env, working_dir) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(data.input.row_id as i64)
        .bind(&data.input.task_id)
        .bind(&data.input.job_id)
        .bind(&data.input.description)
        .bind(&data.input.command)
        .bind(sqlx::types::Json(&data.input.depends_on))
        .bind(&data.input.timeout)
        .bind(&data.input.max_retries)
        .bind(&data.input.retry_delay)
        .bind(sqlx::types::Json(&data.input.env))
        .bind(sqlx::types::Json(&data.input.secret_env))
        .bind(&data.input.working_dir)
        .execute(executor)
        .await?;

        Ok(())
    }

    pub async fn select_tasks<'e, E>(&self, executor: E, data: &SelectTasksData) -> anyhow::Result<Vec<Task>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("SELECT task_id, job_id, description, command, depends_on, timeout, max_retries, retry_delay, env, secret_env, working_dir FROM mem.task WHERE 1=1");

        if let Some(task_id) = &data.filter.task_id {
            query_builder.push(" AND task_id = ");
            query_builder.push_bind(task_id);
        }

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectTasksDataSort::TaskId => {
                    query_builder.push(" ORDER BY task_id ASC");
                }
                SelectTasksDataSort::RowId => {
                    query_builder.push(" ORDER BY row_id ASC");
                }
            }
        }

        if let Some(limit) = data.limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit as i64);
        }

        if let Some(offset) = data.offset {
            query_builder.push(" OFFSET ");
            query_builder.push_bind(offset as i64);
        }

        let tasks = query_builder
            .build_query_as::<Task>()
            .fetch_all(executor)
            .await?;

        Ok(tasks)
    }

    pub async fn select_task<'e, E>(&self, executor: E, data: &SelectTasksData) -> anyhow::Result<Option<Task>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let tasks = self.select_tasks(executor, data).await?;

        Ok(tasks.into_iter().next())
    }
}
