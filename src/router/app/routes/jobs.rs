use axum::extract::{State, Query};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;
use serde::Deserialize;

use crate::router::app::middlewares::session::SignedIn;
use crate::declared::Job;
use crate::router::app::app_state::AppState;

pub struct JobEntry {
    pub job_id: String,
    pub name: String,
    pub description: String,
    pub task_count: String,
}

#[derive(Template)]
#[template(path = "routes/jobs.html")]
struct JobsRouteTemplate {
    current_route: &'static str,
    /// Who is signed in, for the masthead; None when `[auth]` is not configured.
    signed_in_as: Option<String>,
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
    Query(query): Query<JobsRouteQuery>,
    signed_in: Option<Extension<SignedIn>>,
) -> impl IntoResponse {
    let page_size = state.toolkit.app_config.ui.page_size;

    let page = query.page.unwrap_or(1).max(1);
    let offset = (page - 1) * page_size;

    // A cleared search box posts an empty value, which means no filter rather than a
    // filter on the empty name.
    let name_like = query.job_name_like.filter(|name| !name.is_empty());

    let name_like_lowercase = name_like.as_deref().map(str::to_lowercase);

    let matching: Vec<&Job> = state.declared.jobs
        .iter()
        .filter(|job| match &name_like_lowercase {
            Some(name) => job.name.to_lowercase().contains(name),
            None => true,
        })
        .collect();

    let has_next_page = matching.len() > (offset + page_size) as usize;

    let entries = matching
        .into_iter()
        .skip(offset as usize)
        .take(page_size as usize)
        .map(|job| {
            let task_count = state.declared.tasks_of(&job.job_id).len();

            JobEntry {
                job_id: job.job_id.clone(),
                name: job.name.clone(),
                description: job.description.clone(),
                task_count: format!("{} task{}", task_count, if task_count == 1 { "" } else { "s" }),
            }
        })
        .collect();

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
        signed_in_as: signed_in.map(|Extension(signed_in)| signed_in.subject),
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
