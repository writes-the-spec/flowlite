use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertScheduleJobDataInput {
    pub row_id: u64,
    pub schedule_id: String,
    pub job_id: String,
    pub parameters: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertScheduleJobData {
    pub input: InsertScheduleJobDataInput,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SelectScheduleJobsDataSort {
    RowId,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SelectScheduleJobsDataFilter {
    pub schedule_id: Option<String>,
    pub job_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectScheduleJobsData {
    pub filter: SelectScheduleJobsDataFilter,
    pub sort: Option<SelectScheduleJobsDataSort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct ScheduleJob {
    pub schedule_id: String,
    pub job_id: String,
    pub parameters: Option<String>,
}


impl CRUD {
    pub async fn insert_schedule_job<'e, E>(&self, executor: E, data: &InsertScheduleJobData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO mem.schedule_job (row_id, schedule_id, job_id, parameters) VALUES (?, ?, ?, ?)"
        )
        .bind(data.input.row_id as i64)
        .bind(&data.input.schedule_id)
        .bind(&data.input.job_id)
        .bind(&data.input.parameters)
        .execute(executor)
        .await?;

        Ok(())
    }

    pub async fn select_schedule_jobs<'e, E>(&self, executor: E, data: &SelectScheduleJobsData) -> anyhow::Result<Vec<ScheduleJob>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("SELECT schedule_id, job_id, parameters FROM mem.schedule_job WHERE 1=1");

        if let Some(schedule_id) = &data.filter.schedule_id {
            query_builder.push(" AND schedule_id = ");
            query_builder.push_bind(schedule_id);
        }

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectScheduleJobsDataSort::RowId => {
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

        let schedule_jobs = query_builder
            .build_query_as::<ScheduleJob>()
            .fetch_all(executor)
            .await?;

        Ok(schedule_jobs)
    }

    pub async fn select_schedule_job_internal<'e, E>(&self, executor: E, data: &SelectScheduleJobsData) -> anyhow::Result<Option<ScheduleJob>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let schedule_jobs = self.select_schedule_jobs(executor, data).await?;

        Ok(schedule_jobs.into_iter().next())
    }
}
