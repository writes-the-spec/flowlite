use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;


/// Which of an attempt's two streams a chunk came from. They stay apart because a task
/// that failed usually explains itself on stderr while stdout still holds whatever it
/// managed to produce.
#[derive(Debug, Serialize, Deserialize, sqlx::Type, Clone, Copy, PartialEq, Eq, Hash)]
#[sqlx(rename_all = "lowercase")]
pub enum TaskRunAttemptOutputStream {
    Stdout,
    Stderr,
}


impl std::fmt::Display for TaskRunAttemptOutputStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskRunAttemptOutputStream::Stdout => write!(f, "stdout"),
            TaskRunAttemptOutputStream::Stderr => write!(f, "stderr"),
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskRunAttemptOutputDataInput {
    pub task_run_attempt_id: i64,
    pub task_run_id: i64,
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub stream: TaskRunAttemptOutputStream,
    pub content: String,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskRunAttemptOutputData {
    pub input: InsertTaskRunAttemptOutputDataInput,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectTaskRunAttemptOutputsDataFilter {
    pub id: Option<i64>,
    pub task_run_attempt_id: Option<i64>,
    pub task_run_id: Option<i64>,
    pub job_run_id: Option<i64>,
    pub job_id: Option<String>,
    pub task_id: Option<String>,
    pub stream: Option<TaskRunAttemptOutputStream>,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectTaskRunAttemptOutputsData {
    pub filter: SelectTaskRunAttemptOutputsDataFilter,
    pub sort: Option<SelectTaskRunAttemptOutputsDataSort>,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SelectTaskRunAttemptOutputsDataSort {
    Id,
}


#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct TaskRunAttemptOutput {
    pub id: i64,
    pub task_run_attempt_id: i64,
    pub task_run_id: i64,
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub stream: TaskRunAttemptOutputStream,
    pub created_at: DateTime<Utc>,
    pub content: String,
}


/// One attempt's two streams, assembled from its chunks.
///
/// Both are plain Strings, and `Default` is what keeps them that way: an attempt with no
/// rows printed nothing, which is empty output rather than unknown output. Without that,
/// "no rows" would become a second spelling of "no output" and every call site would have
/// to invent a meaning for it — the thing the NOT NULL columns this replaces prevented.
#[derive(Debug, Default, Clone)]
pub struct TaskRunAttemptOutputStreams {
    pub stdout: String,
    pub stderr: String,
}


/// One String per stream per attempt, concatenated in write order.
///
/// Relies on the rows arriving sorted by `id`, which is write order. That is a contract
/// with `select_task_run_attempt_outputs` rather than an accident, and it is what keeps
/// this a single pass with no per-group sort.
pub fn group_task_run_attempt_output(
    rows: Vec<TaskRunAttemptOutput>,
) -> HashMap<i64, TaskRunAttemptOutputStreams> {

    let mut grouped: HashMap<i64, TaskRunAttemptOutputStreams> = HashMap::new();

    for row in rows {
        let streams = grouped.entry(row.task_run_attempt_id).or_default();

        match row.stream {
            TaskRunAttemptOutputStream::Stdout => streams.stdout.push_str(&row.content),
            TaskRunAttemptOutputStream::Stderr => streams.stderr.push_str(&row.content),
        }
    }

    grouped
}


impl CRUD {

    pub async fn insert_task_run_attempt_output<'e, E>(&self, executor: E, data: &InsertTaskRunAttemptOutputData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let res = sqlx::query(
            "INSERT INTO task_run_attempt_output (task_run_attempt_id, task_run_id, job_run_id, job_id, task_id, stream, created_at, content) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
            .bind(data.input.task_run_attempt_id)
            .bind(data.input.task_run_id)
            .bind(data.input.job_run_id)
            .bind(&data.input.job_id)
            .bind(&data.input.task_id)
            .bind(&data.input.stream)
            .bind(self.toolkit.get_current_ts())
            .bind(&data.input.content)
            .execute(executor)
            .await?;

        Ok(res.last_insert_rowid())
    }

    pub async fn select_task_run_attempt_outputs<'e, E>(&self, executor: E, data: &SelectTaskRunAttemptOutputsData) -> anyhow::Result<Vec<TaskRunAttemptOutput>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id, task_run_attempt_id, task_run_id, job_run_id, job_id, task_id, stream, created_at, content FROM task_run_attempt_output WHERE 1=1"
        );

        if let Some(id) = &data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(task_run_attempt_id) = &data.filter.task_run_attempt_id {
            query_builder.push(" AND task_run_attempt_id = ");
            query_builder.push_bind(task_run_attempt_id);
        }

        if let Some(task_run_id) = &data.filter.task_run_id {
            query_builder.push(" AND task_run_id = ");
            query_builder.push_bind(task_run_id);
        }

        if let Some(job_run_id) = &data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
        }

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        if let Some(task_id) = &data.filter.task_id {
            query_builder.push(" AND task_id = ");
            query_builder.push_bind(task_id);
        }

        if let Some(stream) = &data.filter.stream {
            query_builder.push(" AND stream = ");
            query_builder.push_bind(stream);
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectTaskRunAttemptOutputsDataSort::Id => {
                    query_builder.push(" ORDER BY id");
                }
            }
        }

        let task_run_attempt_outputs = query_builder
            .build_query_as::<TaskRunAttemptOutput>()
            .fetch_all(executor)
            .await?;

        Ok(task_run_attempt_outputs)
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::{TaskRunAttempt, TaskRunAttemptStatus};
    use crate::test_support::TestDb;

    async fn attempt(db: &TestDb) -> TaskRunAttempt {
        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;

        db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await
    }

    /// Carries the parent ids off the attempt, the way the monitor does.
    async fn insert(
        db: &TestDb,
        task_run_attempt: &TaskRunAttempt,
        stream: TaskRunAttemptOutputStream,
        content: &str,
    ) {
        db.crud.insert_task_run_attempt_output(
            &*db.conn_pool,
            &InsertTaskRunAttemptOutputData {
                input: InsertTaskRunAttemptOutputDataInput {
                    task_run_attempt_id: task_run_attempt.id,
                    task_run_id: task_run_attempt.task_run_id,
                    job_run_id: task_run_attempt.job_run_id,
                    job_id: task_run_attempt.job_id.clone(),
                    task_id: task_run_attempt.task_id.clone(),
                    stream,
                    content: content.to_string(),
                },
            },
        ).await.unwrap();
    }

    async fn select(db: &TestDb, filter: SelectTaskRunAttemptOutputsDataFilter) -> Vec<TaskRunAttemptOutput> {
        db.crud.select_task_run_attempt_outputs(
            &*db.conn_pool,
            &SelectTaskRunAttemptOutputsData {
                filter,
                sort: Some(SelectTaskRunAttemptOutputsDataSort::Id),
            },
        ).await.unwrap()
    }

    fn filter() -> SelectTaskRunAttemptOutputsDataFilter {
        SelectTaskRunAttemptOutputsDataFilter {
            id: None,
            task_run_attempt_id: None,
            task_run_id: None,
            job_run_id: None,
            job_id: None,
            task_id: None,
            stream: None,
        }
    }

    /// `id` is the ordering, so chunks written in sequence read back in sequence. That is
    /// what lets the grouping helper concatenate in one pass with no per-group sort.
    #[tokio::test]
    async fn chunks_read_back_in_write_order() {
        let db = TestDb::new().await;
        let task_run_attempt = attempt(&db).await;

        insert(&db, &task_run_attempt, TaskRunAttemptOutputStream::Stdout, "one\n").await;
        insert(&db, &task_run_attempt, TaskRunAttemptOutputStream::Stdout, "two\n").await;

        let rows = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            task_run_attempt_id: Some(task_run_attempt.id),
            ..filter()
        }).await;

        let streams = group_task_run_attempt_output(rows)
            .remove(&task_run_attempt.id)
            .unwrap();

        assert_eq!(streams.stdout, "one\ntwo\n");
        assert_eq!(streams.stderr, "");
    }

    /// The two streams stay apart, which is the whole reason the column pair existed.
    #[tokio::test]
    async fn the_streams_are_grouped_apart() {
        let db = TestDb::new().await;
        let task_run_attempt = attempt(&db).await;

        insert(&db, &task_run_attempt, TaskRunAttemptOutputStream::Stdout, "out").await;
        insert(&db, &task_run_attempt, TaskRunAttemptOutputStream::Stderr, "err").await;

        let rows = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            task_run_attempt_id: Some(task_run_attempt.id),
            ..filter()
        }).await;

        let streams = group_task_run_attempt_output(rows)
            .remove(&task_run_attempt.id)
            .unwrap();

        assert_eq!(streams.stdout, "out");
        assert_eq!(streams.stderr, "err");
    }

    /// `task_run_id` is what the task-run page filters on: every attempt of one task run
    /// in one query, which is the N+1 the denormalized column exists to avoid.
    #[tokio::test]
    async fn task_run_id_selects_every_attempt_of_one_task_run() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;

        let first = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Failed).await;
        let second = db.insert_task_run_attempt(&task_run, 2, TaskRunAttemptStatus::Running).await;

        insert(&db, &first, TaskRunAttemptOutputStream::Stdout, "first").await;
        insert(&db, &second, TaskRunAttemptOutputStream::Stdout, "second").await;

        let grouped = group_task_run_attempt_output(
            select(&db, SelectTaskRunAttemptOutputsDataFilter {
                task_run_id: Some(task_run.id),
                ..filter()
            }).await
        );

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped.get(&first.id).unwrap().stdout, "first");
        assert_eq!(grouped.get(&second.id).unwrap().stdout, "second");
    }

    /// `job_run_id` narrowed by `task_id` is what `job-run logs --task` filters on, and it
    /// has to read only that task's output rather than the whole run's.
    #[tokio::test]
    async fn job_run_id_and_task_id_narrow_to_one_task() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let wanted_task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let other_task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;

        let wanted = db.insert_task_run_attempt(&wanted_task_run, 1, TaskRunAttemptStatus::Running).await;
        let other = db.insert_task_run_attempt(&other_task_run, 1, TaskRunAttemptStatus::Running).await;

        insert(&db, &wanted, TaskRunAttemptOutputStream::Stdout, "wanted").await;
        insert(&db, &other, TaskRunAttemptOutputStream::Stdout, "other").await;

        let whole_run = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            job_run_id: Some(job_run.id),
            ..filter()
        }).await;

        let one_task = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            job_run_id: Some(job_run.id),
            task_id: Some(wanted.task_id.clone()),
            ..filter()
        }).await;

        assert_eq!(whole_run.len(), 2);
        assert_eq!(one_task.len(), 1);
        assert_eq!(one_task[0].content, "wanted");
    }

    /// The parent ids are carried, not left to a join - a row knows every run it belongs to.
    #[tokio::test]
    async fn a_row_carries_every_parent_id() {
        let db = TestDb::new().await;
        let task_run_attempt = attempt(&db).await;

        insert(&db, &task_run_attempt, TaskRunAttemptOutputStream::Stdout, "out").await;

        let rows = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            task_run_attempt_id: Some(task_run_attempt.id),
            ..filter()
        }).await;

        assert_eq!(rows[0].task_run_id, task_run_attempt.task_run_id);
        assert_eq!(rows[0].job_run_id, task_run_attempt.job_run_id);
        assert_eq!(rows[0].job_id, task_run_attempt.job_id);
        assert_eq!(rows[0].task_id, task_run_attempt.task_id);
    }

    /// An attempt that printed nothing has empty output, not unknown output.
    #[tokio::test]
    async fn an_attempt_with_no_rows_groups_to_empty_streams() {
        let streams = group_task_run_attempt_output(Vec::new())
            .remove(&1)
            .unwrap_or_default();

        assert_eq!(streams.stdout, "");
        assert_eq!(streams.stderr, "");
    }
}
