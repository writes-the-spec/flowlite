use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;
use sqlx::types::chrono::NaiveDate;

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertScheduleDataInput {
    pub row_id: u64,
    pub schedule_id: String,
    pub name: String,
    pub description: String,
    pub cron: CronSchedule,
    pub timezone: Tz,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub disabled: bool,
    pub next_run: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertScheduleData {
    pub input: InsertScheduleDataInput,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SelectSchedulesDataSort {
    Alphabetical,
    RowId,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SelectSchedulesDataFilter {
    pub schedule_id: Option<String>,
    pub name_like: Option<String>,
    pub next_run_lt: Option<DateTime<Utc>>,
    pub disabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectSchedulesData {
    pub filter: SelectSchedulesDataFilter,
    pub sort: Option<SelectSchedulesDataSort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateSchedulesDataFilter {
    pub schedule_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateSchedulesDataInput {
    pub next_run: Option<Option<DateTime<Utc>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateSchedulesData {
    pub filter: UpdateSchedulesDataFilter,
    pub input: UpdateSchedulesDataInput,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct Schedule {
    pub schedule_id: String,
    pub name: String,
    pub description: String,
    pub cron: String,
    pub timezone: String,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub disabled: bool,
    pub next_run: Option<DateTime<Utc>>,
}


impl CRUD {
    pub async fn insert_schedule<'e, E>(&self, executor: E, data: &InsertScheduleData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO mem.schedule (row_id, schedule_id, name, description, cron, timezone, start_date, end_date, disabled, next_run) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(data.input.row_id as i64)
        .bind(&data.input.schedule_id)
        .bind(&data.input.name)
        .bind(&data.input.description)
        .bind(&data.input.cron.to_string())
        .bind(&data.input.timezone.to_string())
        .bind(&data.input.start_date)
        .bind(&data.input.end_date)
        .bind(if data.input.disabled { 1 } else { 0 })
        .bind(&data.input.next_run)
        .execute(executor)
        .await?;

        Ok(())
    }

    pub async fn select_schedules<'e, E>(&self, executor: E, data: &SelectSchedulesData) -> anyhow::Result<Vec<Schedule>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("SELECT schedule_id, name, description, cron, timezone, start_date, end_date, disabled, next_run FROM mem.schedule WHERE 1=1");

        if let Some(schedule_id) = &data.filter.schedule_id {
            query_builder.push(" AND schedule_id = ");
            query_builder.push_bind(schedule_id);
        }

        if let Some(name) = &data.filter.name_like {
            query_builder.push(" AND name LIKE ");
            query_builder.push_bind(format!("%{}%", name));
        }

        if let Some(next_run_lt) = &data.filter.next_run_lt {
            query_builder.push(" AND next_run < ");
            query_builder.push_bind(next_run_lt);
        }

        if let Some(disabled) = data.filter.disabled {
            query_builder.push(" AND disabled = ");
            query_builder.push_bind(if disabled { 1 } else { 0 });
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectSchedulesDataSort::Alphabetical => {
                    query_builder.push(" ORDER BY name ASC");
                }
                SelectSchedulesDataSort::RowId => {
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

        let schedules = query_builder
            .build_query_as::<Schedule>()
            .fetch_all(executor)
            .await?;

        Ok(schedules)
    }

    pub async fn select_schedule<'e, E>(&self, executor: E, data: &SelectSchedulesData) -> anyhow::Result<Option<Schedule>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let schedules = self.select_schedules(executor, data).await?;

        Ok(schedules.into_iter().next())
    }

    pub async fn update_schedules<'e, E>(&self, executor: E, data: &UpdateSchedulesData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("UPDATE mem.schedule SET ");

        let mut separated = query_builder.separated(", ");

        if let Some(next_run) = &data.input.next_run {
            separated.push("next_run = ");
            separated.push_bind_unseparated(next_run);
        }

        if data.input.next_run.is_none() {
            return Ok(());
        }

        query_builder.push(" WHERE 1=1");

        if let Some(schedule_id) = &data.filter.schedule_id {
            query_builder.push(" AND schedule_id = ");
            query_builder.push_bind(schedule_id);
        }

        query_builder
            .build()
            .execute(executor)
            .await?;

        Ok(())
    }
}
