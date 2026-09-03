use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunStopDataInput {
    pub job_run_id: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunStopData {
    pub input: InsertJobRunStopDataInput,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunStopsDataFilter {
    pub id: Option<i64>,
    pub job_run_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunStopsData {
    pub filter: SelectJobRunStopsDataFilter,
    pub sort: Option<SelectJobRunStopsSort>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SelectJobRunStopsSort {
    CreatedAtDesc,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct JobRunStop {
    pub id: i64,
    pub job_run_id: i64,
    pub created_at: DateTime<Utc>,
}

impl CRUD {
    pub async fn insert_job_run_stop<'e, E>(&self, executor: E, data: &InsertJobRunStopData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let res = sqlx::query(
            "INSERT INTO job_run_stop (job_run_id, created_at) VALUES (?, ?)"
        )
            .bind(data.input.job_run_id)
            .bind(self.toolkit.get_current_ts())
            .execute(executor)
            .await?;

        Ok(res.last_insert_rowid())
    }

    pub async fn select_job_run_stop<'e, E>(&self, executor: E, data: &SelectJobRunStopsData) -> anyhow::Result<Option<JobRunStop>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let aborts = self.select_job_run_stops(executor, data).await?;
        Ok(aborts.into_iter().next())
    }

    pub async fn select_job_run_stops<'e, E>(&self, executor: E, data: &SelectJobRunStopsData) -> anyhow::Result<Vec<JobRunStop>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id, job_run_id, created_at FROM job_run_stop WHERE 1=1"
        );

        if let Some(id) = &data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(job_run_id) = &data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectJobRunStopsSort::CreatedAtDesc => {
                    query_builder.push(" ORDER BY created_at DESC");
                }
            }
        }

        if let Some(limit) = data.limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit);
        }

        if let Some(offset) = data.offset {
            query_builder.push(" OFFSET ");
            query_builder.push_bind(offset);
        }

        let aborts = query_builder
            .build_query_as::<JobRunStop>()
            .fetch_all(executor)
            .await?;

        Ok(aborts)
    }
    
}
