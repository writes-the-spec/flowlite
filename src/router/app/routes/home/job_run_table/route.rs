use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;
use axum::extract::{State, Query};
use chrono::{DateTime, Utc};

use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::router::app::app_state::AppState;
use crate::router::app::format;
use crate::router::app::routes::home::route::{HomeQuery, Pagination, runs_href};

pub struct JobRunDisplay {
    pub id: i64,
    pub job_id: String,
    pub job_name: String,
    pub status: JobRunStatus,
    pub status_word: &'static str,
    pub created_at: String,
    pub duration: String,
    pub span_pct: i64,
}

#[derive(Template)]
#[template(path = "routes/home/job_run_table/route.html")]
struct JobRunTableTemplate {
    job_runs: Vec<JobRunDisplay>,
    pagination: Pagination,
    prev_href: Option<String>,
    next_href: Option<String>,
}

/// A run that is still going is measured against now, so its bar keeps growing while
/// the table refreshes itself.
fn elapsed_seconds(run: &JobRun, now: DateTime<Utc>) -> Option<i64> {
    let started_at = run.started_at?;
    let finished_at = run.finished_at.unwrap_or(now);

    Some(finished_at.signed_duration_since(started_at).num_seconds().max(0))
}

pub async fn job_run_table_route(
    Extension(crud): Extension<CRUD>,
    State(state): State<AppState>,
    Query(query): Query<HomeQuery>,
) -> impl IntoResponse {
    let page = query.page();
    let page_size = query.page_size(&state.toolkit.app_config);
    let filter_job_id = query.job_id();
    let offset = (page - 1) * page_size;

    let job_runs = crud.select_job_runs(&*state.conn_pool, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: None,
            job_id: filter_job_id.clone(),
            status: query.filter_status,
        },
        sort: Some(SelectJobRunsDataSort::IdDesc),
        limit: Some((page_size + 1) as i64),
        offset: Some(offset as i64),
    }).await.unwrap_or_default();

    let has_next_page = job_runs.len() > page_size;
    let job_runs = if has_next_page {
        job_runs.into_iter().take(page_size).collect::<Vec<_>>()
    } else {
        job_runs
    };

    let now = Utc::now();
    let elapsed: Vec<Option<i64>> = job_runs.iter().map(|run| elapsed_seconds(run, now)).collect();
    let slowest_seconds = elapsed.iter().flatten().copied().max().unwrap_or(0);

    let job_runs_display = job_runs.into_iter().zip(elapsed).map(|(run, seconds)| {
        JobRunDisplay {
            id: run.id,
            job_id: run.job_id,
            job_name: run.job_name,
            status: run.status,
            // Success stays wordless in the list: the green mark says it, and the words
            // that remain are the ones worth scanning for.
            status_word: match run.status {
                JobRunStatus::Succeeded => "",
                status => format::job_run_word(status),
            },
            created_at: format::short_timestamp(run.created_at),
            duration: seconds.map(format::duration).unwrap_or_else(|| "—".to_string()),
            span_pct: match seconds {
                Some(seconds) if slowest_seconds > 0 => seconds * 100 / slowest_seconds,
                _ => 0,
            },
        }
    }).collect();

    let prev_href = if page > 1 {
        Some(runs_href(page - 1, page_size, filter_job_id.as_deref(), query.filter_status))
    } else {
        None
    };

    let next_href = if has_next_page {
        Some(runs_href(page + 1, page_size, filter_job_id.as_deref(), query.filter_status))
    } else {
        None
    };

    let template = JobRunTableTemplate {
        job_runs: job_runs_display,
        pagination: Pagination {
            current_page: page,
            page_size,
            filter_job_id,
            filter_status: query.filter_status,
        },
        prev_href,
        next_href,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
