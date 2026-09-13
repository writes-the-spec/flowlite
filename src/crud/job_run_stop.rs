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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteJobRunStopsDataFilter {
    pub id: Option<i64>,
    pub job_run_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteJobRunStopsData {
    pub filter: DeleteJobRunStopsDataFilter,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
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

    /// Deletes every row in `job_run_stop` matching `data.filter`. An entirely empty filter
    /// matches every row and so deletes the whole table — exact parity with an empty
    /// select filter, and the caller's business, not this method's.
    pub async fn delete_job_run_stops<'e, E>(&self, executor: E, data: &DeleteJobRunStopsData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM job_run_stop WHERE 1=1"
        );

        if let Some(id) = &data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(job_run_id) = &data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
        }

        query_builder.build().execute(executor).await?;

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::JobRunStatus;
    use crate::test_support::TestDb;

    async fn select(db: &TestDb, filter: SelectJobRunStopsDataFilter) -> Vec<JobRunStop> {
        db.crud.select_job_run_stops(&*db.conn_pool, &SelectJobRunStopsData {
            filter,
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap()
    }

    fn empty_filter() -> SelectJobRunStopsDataFilter {
        SelectJobRunStopsDataFilter { id: None, job_run_id: None }
    }

    /// `job_run_id` really filters: deleting by one job run's id only removes its stop,
    /// leaving a neighbouring job run's stop untouched.
    ///
    /// A throwaway job run with no stop of its own is inserted first so `job_run.id` and
    /// `job_run_stop.id` diverge (2 and 1, not both 1) - otherwise a delete that filtered on
    /// the stop's own `id` instead of `job_run_id` would still happen to hit the right row
    /// and this test would not notice.
    #[tokio::test]
    async fn delete_job_run_stops_filters_by_job_run_id() {

        let db = TestDb::new().await;

        db.insert_job_run(JobRunStatus::Failed).await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;
        db.insert_job_run_stop(job_run.id).await;

        let other_job_run = db.insert_job_run(JobRunStatus::Failed).await;
        db.insert_job_run_stop(other_job_run.id).await;

        db.crud.delete_job_run_stops(&*db.conn_pool, &DeleteJobRunStopsData {
            filter: DeleteJobRunStopsDataFilter { id: None, job_run_id: Some(job_run.id) },
        }).await.unwrap();

        assert!(select(&db, SelectJobRunStopsDataFilter { job_run_id: Some(job_run.id), ..empty_filter() }).await.is_empty());
        assert!(!select(&db, SelectJobRunStopsDataFilter { job_run_id: Some(other_job_run.id), ..empty_filter() }).await.is_empty());
    }

    /// The decided behaviour, pinned so a future guard cannot be added silently: an
    /// entirely empty filter matches every row and so deletes the whole table.
    #[tokio::test]
    async fn an_empty_filter_deletes_every_job_run_stop() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;
        db.insert_job_run_stop(job_run.id).await;

        db.crud.delete_job_run_stops(&*db.conn_pool, &DeleteJobRunStopsData {
            filter: DeleteJobRunStopsDataFilter { id: None, job_run_id: None },
        }).await.unwrap();

        assert!(select(&db, empty_filter()).await.is_empty());
    }
}
