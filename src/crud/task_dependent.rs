use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskDependentDataInput {
    pub row_id: u64,
    pub job_id: String,
    pub task_id: String,
    pub dependent_task_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskDependentData {
    pub input: InsertTaskDependentDataInput,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SelectTaskDependentsDataSort {
    RowId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTaskDependentsDataFilter {
    pub job_id: Option<String>,
    pub task_id: Option<String>,
    pub dependent_task_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTaskDependentsData {
    pub filter: SelectTaskDependentsDataFilter,
    pub sort: Option<SelectTaskDependentsDataSort>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct TaskDependent {
    pub job_id: String,
    pub task_id: String,
    pub dependent_task_id: String,
}


impl CRUD {
    pub async fn insert_task_dependent<'e, E>(&self, executor: E, data: &InsertTaskDependentData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO mem.task_dependent (row_id, job_id, task_id, dependent_task_id) VALUES (?, ?, ?, ?)"
        )
        .bind(data.input.row_id as i64)
        .bind(&data.input.job_id)
        .bind(&data.input.task_id)
        .bind(&data.input.dependent_task_id)
        .execute(executor)
        .await?;

        Ok(())
    }

    pub async fn select_task_dependents<'e, E>(&self, executor: E, data: &SelectTaskDependentsData) -> anyhow::Result<Vec<TaskDependent>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("SELECT job_id, task_id, dependent_task_id FROM mem.task_dependent WHERE 1=1");

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        if let Some(task_id) = &data.filter.task_id {
            query_builder.push(" AND task_id = ");
            query_builder.push_bind(task_id);
        }

        if let Some(dependent_task_id) = &data.filter.dependent_task_id {
            query_builder.push(" AND dependent_task_id = ");
            query_builder.push_bind(dependent_task_id);
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectTaskDependentsDataSort::RowId => {
                    query_builder.push(" ORDER BY row_id ASC");
                }
            }
        }

        let task_dependents = query_builder
            .build_query_as::<TaskDependent>()
            .fetch_all(executor)
            .await?;

        Ok(task_dependents)
    }
}
