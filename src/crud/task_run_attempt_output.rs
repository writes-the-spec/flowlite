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
    /// Every attempt on one page at once. Both readers of this table list the attempts
    /// before they want their output, so filtering on the ids they already hold is what
    /// keeps the task-run view and `job-run logs` one query rather than one per attempt.
    pub task_run_attempt_ids: Option<Vec<i64>>,
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
            "INSERT INTO task_run_attempt_output (task_run_attempt_id, stream, created_at, content) VALUES (?, ?, ?, ?)"
        )
            .bind(data.input.task_run_attempt_id)
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
            "SELECT id, task_run_attempt_id, stream, created_at, content FROM task_run_attempt_output WHERE 1=1"
        );

        if let Some(id) = &data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(task_run_attempt_id) = &data.filter.task_run_attempt_id {
            query_builder.push(" AND task_run_attempt_id = ");
            query_builder.push_bind(task_run_attempt_id);
        }

        if let Some(task_run_attempt_ids) = &data.filter.task_run_attempt_ids {

            match task_run_attempt_ids.is_empty() {
                // `IN ()` is a syntax error, and an empty list is a real case: a task run
                // with no attempts yet asks for the output of nothing.
                true => {
                    query_builder.push(" AND 0 = 1");
                },
                false => {
                    query_builder.push(" AND task_run_attempt_id IN (");

                    // Scoped so the Separated borrow ends before the paren is closed.
                    {
                        let mut separated = query_builder.separated(", ");

                        for task_run_attempt_id in task_run_attempt_ids {
                            separated.push_bind(*task_run_attempt_id);
                        }
                    }

                    query_builder.push(")");
                },
            }
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

    async fn insert(db: &TestDb, task_run_attempt_id: i64, stream: TaskRunAttemptOutputStream, content: &str) {
        db.crud.insert_task_run_attempt_output(
            &*db.conn_pool,
            &InsertTaskRunAttemptOutputData {
                input: InsertTaskRunAttemptOutputDataInput {
                    task_run_attempt_id,
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
            task_run_attempt_ids: None,
            stream: None,
        }
    }

    /// `id` is the ordering, so chunks written in sequence read back in sequence. That is
    /// what lets the grouping helper concatenate in one pass with no per-group sort.
    #[tokio::test]
    async fn chunks_read_back_in_write_order() {
        let db = TestDb::new().await;
        let task_run_attempt = attempt(&db).await;

        insert(&db, task_run_attempt.id, TaskRunAttemptOutputStream::Stdout, "one\n").await;
        insert(&db, task_run_attempt.id, TaskRunAttemptOutputStream::Stdout, "two\n").await;

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

        insert(&db, task_run_attempt.id, TaskRunAttemptOutputStream::Stdout, "out").await;
        insert(&db, task_run_attempt.id, TaskRunAttemptOutputStream::Stderr, "err").await;

        let rows = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            task_run_attempt_ids: Some(vec![task_run_attempt.id]),
            ..filter()
        }).await;

        let streams = group_task_run_attempt_output(rows)
            .remove(&task_run_attempt.id)
            .unwrap();

        assert_eq!(streams.stdout, "out");
        assert_eq!(streams.stderr, "err");
    }

    /// The id list is what keeps the two views one query, so it has to hold more than one.
    #[tokio::test]
    async fn the_id_list_groups_several_attempts_from_one_query() {
        let db = TestDb::new().await;
        let first = attempt(&db).await;
        let second = attempt(&db).await;

        insert(&db, first.id, TaskRunAttemptOutputStream::Stdout, "first").await;
        insert(&db, second.id, TaskRunAttemptOutputStream::Stdout, "second").await;

        let rows = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            task_run_attempt_ids: Some(vec![first.id, second.id]),
            ..filter()
        }).await;

        let grouped = group_task_run_attempt_output(rows);

        assert_eq!(grouped.get(&first.id).unwrap().stdout, "first");
        assert_eq!(grouped.get(&second.id).unwrap().stdout, "second");
    }

    /// An empty id list has to mean "no rows" rather than `IN ()`, which is a SQLite
    /// syntax error. A task run with no attempts reaches here.
    #[tokio::test]
    async fn an_empty_id_list_selects_nothing() {
        let db = TestDb::new().await;
        let task_run_attempt = attempt(&db).await;

        insert(&db, task_run_attempt.id, TaskRunAttemptOutputStream::Stdout, "out").await;

        let rows = select(&db, SelectTaskRunAttemptOutputsDataFilter {
            task_run_attempt_ids: Some(Vec::new()),
            ..filter()
        }).await;

        assert!(rows.is_empty());
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
