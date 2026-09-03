use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse, Redirect};
use askama::Template;
use axum::Extension;
use chrono::{DateTime, Utc};
use std::cmp::Ordering;

use crate::crud::CRUD;
use crate::crud::job::{Job, SelectJobsData, SelectJobsDataFilter, SelectJobsDataSort};
use crate::crud::job_run::{JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};
use crate::crud::task_run::{TaskRun, SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRunStatus};
use crate::router::app::app_state::AppState;
use crate::router::app::format;

pub struct JobRunDisplay {
    pub id: i64,
    pub job_id: String,
    pub job_name: String,
    pub status: JobRunStatus,
    pub status_word: &'static str,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration: Option<String>,
}

/// One task's bar on the run timeline, placed as a percentage of the run's own window so
/// the whole run always fills the track exactly once.
pub struct Lane {
    pub task_run_id: i64,
    pub task_id: String,
    pub status: TaskRunStatus,
    pub depends_on: Option<String>,
    pub started: bool,
    pub start_pct: i64,
    pub span_pct: i64,
    pub duration: String,
    pub idle_label: &'static str,
}

pub struct Tick {
    pub at_pct: i64,
    pub label: String,
}

#[derive(Template)]
#[template(path = "routes/job_runs/job_run_id/route.html")]
struct JobRunIdRouteTemplate {
    current_route: &'static str,
    job_run: JobRunDisplay,
    job: Option<Job>,
    lanes: Vec<Lane>,
    ticks: Vec<Tick>,
    task_count: String,
    polling: bool,
    stoppable: bool,
}

fn idle_label(status: TaskRunStatus) -> &'static str {
    match status {
        TaskRunStatus::Pending => "waiting",
        TaskRunStatus::Skipped => "skipped",
        TaskRunStatus::Aborted => "aborted",
        _ => "never started",
    }
}

/// Four intervals only carry distinct labels once the run is a few seconds long; below
/// that the axis collapses to its two endpoints.
fn build_ticks(window_seconds: i64) -> Vec<Tick> {
    let steps = if window_seconds >= 4 { 4 } else { 1 };

    (0..=steps).map(|step| Tick {
        at_pct: step * 100 / steps,
        label: format::duration(window_seconds * step / steps),
    }).collect()
}

fn build_lane(
    task_run: TaskRun,
    depends_on: Option<String>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    window_ms: i64,
) -> Lane {
    let Some(started_at) = task_run.started_at else {
        return Lane {
            task_run_id: task_run.id,
            task_id: task_run.task_id,
            status: task_run.status,
            depends_on,
            started: false,
            start_pct: 0,
            span_pct: 0,
            duration: "—".to_string(),
            idle_label: idle_label(task_run.status),
        };
    };

    let finished_at = task_run.finished_at.unwrap_or(window_end);

    let offset_ms = started_at.signed_duration_since(window_start).num_milliseconds();
    let elapsed_ms = finished_at.signed_duration_since(started_at).num_milliseconds();

    let start_pct = (offset_ms * 100 / window_ms).clamp(0, 100);
    let span_pct = (elapsed_ms * 100 / window_ms).clamp(0, 100 - start_pct);

    Lane {
        task_run_id: task_run.id,
        task_id: task_run.task_id,
        status: task_run.status,
        depends_on,
        started: true,
        start_pct,
        span_pct,
        duration: format::duration(elapsed_ms / 1000),
        idle_label: "",
    }
}

pub async fn job_run_id_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(job_run_id): Path<i64>,
) -> impl IntoResponse {
    let conn = &*state.conn_pool;

    let job_run = crud.select_job_runs(conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: Some(job_run_id),
            job_id: None,
            status: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await.unwrap_or_default().into_iter().next();

    let job_run = match job_run {
        Some(job_run) => job_run,
        None => return Html("Job Run not found".to_string()).into_response(),
    };

    let job = crud.select_job(conn, &SelectJobsData {
        filter: SelectJobsDataFilter {
            job_id: Some(job_run.job_id.clone()),
            name_like: None,
        },
        sort: Some(SelectJobsDataSort::Alphabetical),
        limit: None,
        offset: None,
    }).await.unwrap_or(None);

    let tasks = crud.select_tasks(conn, &SelectTasksData {
        filter: SelectTasksDataFilter {
            task_id: None,
            job_id: Some(job_run.job_id.clone()),
        },
        sort: Some(SelectTasksDataSort::TaskId),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

    let mut task_runs = crud.select_task_runs(conn, &SelectTaskRunsData {
        filter: SelectTaskRunsDataFilter {
            id: None,
            job_run_id: Some(job_run.id),
            job_id: None,
            task_id: None,
            status: None,
        },
        sort: None,
    }).await.unwrap_or_default();

    // Reading order on a timeline is start time; tasks that never started sink to the bottom.
    task_runs.sort_by(|left, right| match (left.started_at, right.started_at) {
        (Some(left_start), Some(right_start)) => left_start.cmp(&right_start),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }.then_with(|| left.task_id.cmp(&right.task_id)));

    let now = Utc::now();
    let window_start = job_run.started_at.unwrap_or(job_run.created_at);
    let window_end = job_run.finished_at.unwrap_or(now);
    let window_ms = window_end.signed_duration_since(window_start).num_milliseconds().max(1);
    let window_seconds = window_ms / 1000;

    let lanes = task_runs.into_iter().map(|task_run| {
        let depends_on = tasks.iter()
            .find(|task| task.task_id == task_run.task_id)
            .map(|task| task.depends_on.0.join(", "))
            .filter(|depends_on| !depends_on.is_empty());

        build_lane(task_run, depends_on, window_start, window_end, window_ms)
    }).collect::<Vec<_>>();

    let lane_count = lanes.len();

    let duration = job_run.started_at.map(|started_at| {
        let finished_at = job_run.finished_at.unwrap_or(now);
        format::duration(finished_at.signed_duration_since(started_at).num_seconds())
    });

    let template = JobRunIdRouteTemplate {
        current_route: "home",
        job_run: JobRunDisplay {
            id: job_run.id,
            job_id: job_run.job_id,
            job_name: job_run.job_name,
            status: job_run.status,
            status_word: format::job_run_word(job_run.status),
            created_at: format::timestamp(job_run.created_at),
            started_at: job_run.started_at.map(format::timestamp),
            finished_at: job_run.finished_at.map(format::timestamp),
            duration,
        },
        job,
        lanes,
        ticks: build_ticks(window_seconds),
        task_count: format!("{} task{}", lane_count, if lane_count == 1 { "" } else { "s" }),
        polling: matches!(job_run.status, JobRunStatus::Pending | JobRunStatus::Running),
        stoppable: matches!(job_run.status, JobRunStatus::Running),
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}

pub async fn stop_job_run_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(job_run_id): Path<i64>,
) -> impl IntoResponse {
    let result = crud.insert_job_run_stop(&*state.conn_pool, &InsertJobRunStopData {
        input: InsertJobRunStopDataInput {
            job_run_id,
        }
    }).await;

    match result {
        Ok(_) => Redirect::to(&format!("/job-runs/{}", job_run_id)).into_response(),
        Err(err) => {
            eprintln!("Error stopping job run: {}", err);
            Html("Error stopping job run").into_response()
        }
    }
}
