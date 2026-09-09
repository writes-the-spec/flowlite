
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobDataInput {
    pub row_id: u64,
    pub job_id: String,
    pub name: String,
    pub description: String,
    pub max_parallel_runs: u32,
    pub parameters: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
    pub on_failure_emails: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobData {
    pub input: InsertJobDataInput,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SelectJobsDataSort {
    Alphabetical,
    RowId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectJobsDataFilter {
    pub job_id: Option<String>,
    pub name_like: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectJobsData {
    pub filter: SelectJobsDataFilter,
    pub sort: Option<SelectJobsDataSort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct Job {
    pub job_id: String,
    pub name: String,
    pub description: String,
    pub max_parallel_runs: u32,
    pub parameters: sqlx::types::Json<BTreeMap<String, String>>,
    pub env: sqlx::types::Json<BTreeMap<String, String>>,
    pub on_failure_emails: sqlx::types::Json<Vec<String>>,
}


impl CRUD {
    pub async fn insert_job<'e, E>(&self, executor: E, data: &InsertJobData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO mem.job (row_id, job_id, name, description, max_parallel_runs, parameters, env, on_failure_emails) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(data.input.row_id as i64)
        .bind(&data.input.job_id)
        .bind(&data.input.name)
        .bind(&data.input.description)
        .bind(data.input.max_parallel_runs)
        .bind(sqlx::types::Json(&data.input.parameters))
        .bind(sqlx::types::Json(&data.input.env))
        .bind(sqlx::types::Json(&data.input.on_failure_emails))
        .execute(executor)
        .await?;

        Ok(())
    }

    pub async fn select_jobs<'e, E>(&self, executor: E, data: &SelectJobsData) -> anyhow::Result<Vec<Job>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("SELECT job_id, name, description, max_parallel_runs, parameters, env, on_failure_emails FROM mem.job WHERE 1=1");

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        if let Some(name) = &data.filter.name_like {
            query_builder.push(" AND name LIKE ");
            query_builder.push_bind(format!("%{}%", name));
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectJobsDataSort::Alphabetical => {
                    query_builder.push(" ORDER BY name ASC");
                }
                SelectJobsDataSort::RowId => {
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

        let jobs = query_builder
            .build_query_as::<Job>()
            .fetch_all(executor)
            .await?;

        Ok(jobs)
    }

    pub async fn select_job<'e, E>(&self, executor: E, data: &SelectJobsData) -> anyhow::Result<Option<Job>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let jobs = self.select_jobs(executor, data).await?;

        Ok(jobs.into_iter().next())
    }
}
