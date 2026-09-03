use axum::extract::{State, Query};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;
use serde::Deserialize;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter, SelectJobsDataSort};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter};
use crate::router::app::app_state::AppState;

const PAGE_SIZE: u32 = 25;

pub struct JobEntry {
    pub job_id: String,
    pub name: String,
    pub description: String,
    pub task_count: String,
}

#[derive(Template)]
#[template(path = "routes/jobs/route.html")]
struct JobsRouteTemplate {
    current_route: &'static str,
    jobs: Vec<JobEntry>,
    current_name: Option<String>,
    prev_href: Option<String>,
    next_href: Option<String>,
}

#[derive(Deserialize)]
pub struct JobsRouteQuery {
    pub job_name_like: Option<String>,
    pub page: Option<u32>,
}

fn jobs_href(page: u32, name_like: Option<&str>) -> String {
    match name_like {
        Some(name) => format!("/jobs?page={}&job_name_like={}", page, name),
        None => format!("/jobs?page={}", page),
    }
}

pub async fn jobs_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Query(query): Query<JobsRouteQuery>,
) -> impl IntoResponse {
    let conn = &*state.conn_pool;

    let page = query.page.unwrap_or(1).max(1);
    let offset = (page - 1) * PAGE_SIZE;

    // A cleared search box posts an empty value, which means no filter rather than a
    // filter on the empty name.
    let name_like = query.job_name_like.filter(|name| !name.is_empty());

    let jobs = crud.select_jobs(conn, &SelectJobsData {
        filter: SelectJobsDataFilter {
            job_id: None,
            name_like: name_like.clone(),
        },
        sort: Some(SelectJobsDataSort::Alphabetical),
        limit: Some(PAGE_SIZE + 1), // One extra row tells us whether a next page exists.
        offset: Some(offset),
    }).await.unwrap_or_default();

    let has_next_page = jobs.len() > PAGE_SIZE as usize;

    let mut entries = Vec::new();

    for job in jobs.into_iter().take(PAGE_SIZE as usize) {
        let tasks = crud.select_tasks(conn, &SelectTasksData {
            filter: SelectTasksDataFilter {
                task_id: None,
                job_id: Some(job.job_id.clone()),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap_or_default();

        entries.push(JobEntry {
            job_id: job.job_id,
            name: job.name,
            description: job.description,
            task_count: format!("{} task{}", tasks.len(), if tasks.len() == 1 { "" } else { "s" }),
        });
    }

    let prev_href = if page > 1 {
        Some(jobs_href(page - 1, name_like.as_deref()))
    } else {
        None
    };

    let next_href = if has_next_page {
        Some(jobs_href(page + 1, name_like.as_deref()))
    } else {
        None
    };

    let template = JobsRouteTemplate {
        current_route: "jobs",
        jobs: entries,
        current_name: name_like,
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
