use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;
use axum::extract::{State, Query};
use serde::Deserialize;

use crate::router::app::middlewares::session::SignedIn;
use crate::app_config::AppConfig;
use crate::declared::Job;
use crate::crud::job_run::JobRunStatus;
use crate::router::app::app_state::AppState;
use crate::shared::format;

pub const ALL_STATUSES: [JobRunStatus; 9] = JobRunStatus::ALL;


#[derive(Deserialize)]
pub struct HomeQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
    pub filter_job_id: Option<String>,
    pub filter_status: Option<JobRunStatus>,
}

/// The largest `?page_size=` the run list accepts.
const MAX_PAGE_SIZE: usize = 100;

impl HomeQuery {
    pub fn page(&self) -> usize {
        self.page.unwrap_or(1).max(1)
    }

    /// The query string's own value, defaulted by `[ui] page_size` in flowlite.toml and
    /// capped at `MAX_PAGE_SIZE`.
    pub fn page_size(&self, app_config: &AppConfig) -> usize {
        self.page_size
            .unwrap_or(app_config.ui.page_size as usize)
            .clamp(1, MAX_PAGE_SIZE)
    }

    /// The job select posts an empty value for "All jobs", which means no filter at all
    /// rather than a filter on the empty job id.
    pub fn job_id(&self) -> Option<String> {
        self.filter_job_id.clone().filter(|job_id| !job_id.is_empty())
    }
}

pub struct Pagination {
    pub current_page: usize,
    pub page_size: usize,
    pub filter_job_id: Option<String>,
    pub filter_status: Option<JobRunStatus>,
}

/// Trailing query fragment, either empty or starting with `&`, so it can be appended to
/// any run-list URL without the caller tracking where the `?` went.
fn filter_suffix(filter_job_id: Option<&str>, filter_status: Option<JobRunStatus>) -> String {
    let mut suffix = String::new();

    if let Some(job_id) = filter_job_id {
        suffix.push_str(&format!("&filter_job_id={}", job_id));
    }

    if let Some(status) = filter_status {
        suffix.push_str(&format!("&filter_status={}", status));
    }

    suffix
}

pub fn runs_href(
    page: usize,
    page_size: usize,
    filter_job_id: Option<&str>,
    filter_status: Option<JobRunStatus>,
) -> String {
    format!(
        "/?page={}&page_size={}{}",
        page,
        page_size,
        filter_suffix(filter_job_id, filter_status),
    )
}

pub struct StatusChip {
    pub label: &'static str,
    pub status: Option<JobRunStatus>,
    pub href: String,
    pub selected: bool,
}

#[derive(Template)]
#[template(path = "routes/home.html")]
struct HomeRouteTemplate {
    current_route: &'static str,
    /// Who is signed in, for the masthead; None when `[auth]` is not configured.
    signed_in_as: Option<String>,
    page_size: usize,
    refresh_seconds: u32,
    table_href: String,
    jobs: Vec<Job>,
    selected_job_id: Option<String>,
    selected_status: Option<JobRunStatus>,
    status_chips: Vec<StatusChip>,
}

pub async fn home_route(
    State(state): State<AppState>,
    Query(query): Query<HomeQuery>,
    signed_in: Option<Extension<SignedIn>>,
) -> impl IntoResponse {
    let app_config = &state.toolkit.app_config;

    let page = query.page();
    let page_size = query.page_size(app_config);
    let filter_job_id = query.job_id();

    let mut jobs: Vec<Job> = state.declared.jobs.clone();
    jobs.sort_by(|a, b| a.name.cmp(&b.name));

    let mut status_chips = vec![StatusChip {
        label: "all",
        status: None,
        href: runs_href(1, page_size, filter_job_id.as_deref(), None),
        selected: query.filter_status.is_none(),
    }];

    for status in ALL_STATUSES {
        status_chips.push(StatusChip {
            label: format::job_run_word(status),
            status: Some(status),
            href: runs_href(1, page_size, filter_job_id.as_deref(), Some(status)),
            selected: query.filter_status == Some(status),
        });
    }

    let table_href = format!(
        "/job-run-table?page={}&page_size={}{}",
        page,
        page_size,
        filter_suffix(filter_job_id.as_deref(), query.filter_status),
    );

    let template = HomeRouteTemplate {
        current_route: "home",
        signed_in_as: signed_in.map(|Extension(signed_in)| signed_in.subject),
        page_size,
        refresh_seconds: super::REFRESH_INTERVAL_SECONDS,
        table_href,
        jobs,
        selected_job_id: filter_job_id,
        selected_status: query.filter_status,
        status_chips,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
