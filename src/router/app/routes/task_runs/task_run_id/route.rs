use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;

use crate::crud::CRUD;
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRunStatus};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use crate::router::app::app_state::AppState;
use crate::router::app::format;

pub struct TaskRunDisplay {
    pub id: i64,
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub status: TaskRunStatus,
    pub status_word: &'static str,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration: Option<String>,
}

/// One attempt's output. The streams stay separate because a task that failed usually
/// explains itself on stderr while stdout still holds whatever it managed to produce.
pub struct AttemptDisplay {
    pub attempt: u32,
    pub status: TaskRunAttemptStatus,
    pub status_word: &'static str,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration: Option<String>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Template)]
#[template(path = "routes/task_runs/task_run_id/route.html")]
struct TaskRunIdRouteTemplate {
    current_route: &'static str,
    task_run: TaskRunDisplay,
    attempts: Vec<AttemptDisplay>,
    attempt_note: String,
    polling: bool,
}

fn build_attempt(task_run_attempt: TaskRunAttempt) -> AttemptDisplay {
    let duration = task_run_attempt.started_at.map(|started_at| {
        let finished_at = task_run_attempt.finished_at.unwrap_or_else(chrono::Utc::now);
        format::duration(finished_at.signed_duration_since(started_at).num_seconds())
    });

    AttemptDisplay {
        attempt: task_run_attempt.attempt,
        status: task_run_attempt.status,
        status_word: format::task_run_attempt_word(task_run_attempt.status),
        started_at: task_run_attempt.started_at.map(format::timestamp),
        finished_at: task_run_attempt.finished_at.map(format::timestamp),
        duration,
        stdout: task_run_attempt.stdout,
        stderr: task_run_attempt.stderr,
    }
}

pub async fn task_run_id_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(task_run_id): Path<i64>,
) -> impl IntoResponse {
    let conn = &*state.conn_pool;

    let task_run = crud.select_task_run(conn, &SelectTaskRunsData {
        filter: SelectTaskRunsDataFilter {
            id: Some(task_run_id),
            job_run_id: None,
            job_id: None,
            task_id: None,
            status: None,
        },
        sort: None,
    }).await.unwrap_or(None);

    let task_run = match task_run {
        Some(task_run) => task_run,
        None => return Html("Task Run not found".to_string()).into_response(),
    };

    let task_run_attempts = crud.select_task_run_attempts(conn, &SelectTaskRunAttemptsData {
        filter: SelectTaskRunAttemptsDataFilter {
            task_run_id: Some(task_run.id),
            job_run_id: None,
            task_id: None,
            status: None,
        },
        sort: Some(SelectTaskRunAttemptsDataSort::Id),
    }).await.unwrap_or_default();

    let attempts = task_run_attempts.into_iter().map(build_attempt).collect::<Vec<_>>();

    let attempt_note = match attempts.len() {
        0 => "no attempts yet".to_string(),
        1 => "1 attempt".to_string(),
        count => format!("{} attempts, newest last", count),
    };

    let duration = task_run.started_at.map(|started_at| {
        let finished_at = task_run.finished_at.unwrap_or_else(chrono::Utc::now);
        format::duration(finished_at.signed_duration_since(started_at).num_seconds())
    });

    let template = TaskRunIdRouteTemplate {
        current_route: "home",
        task_run: TaskRunDisplay {
            id: task_run.id,
            job_run_id: task_run.job_run_id,
            job_id: task_run.job_id,
            task_id: task_run.task_id,
            status: task_run.status,
            status_word: format::task_run_word(task_run.status),
            started_at: task_run.started_at.map(format::timestamp),
            finished_at: task_run.finished_at.map(format::timestamp),
            duration,
        },
        attempts,
        attempt_note,
        polling: matches!(task_run.status, TaskRunStatus::Pending | TaskRunStatus::Running),
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
