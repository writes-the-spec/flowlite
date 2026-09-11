use std::collections::BTreeMap;
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;
use axum::extract::{State, Query};
use serde::Deserialize;

use crate::app_config::AppConfig;
use crate::crud::CRUD;
use crate::crud::job::{Job, SelectJobsData, SelectJobsDataFilter, SelectJobsDataSort};
use crate::crud::job_run::JobRunStatus;
use crate::router::app::app_state::AppState;
use crate::router::app::format;
use crate::router::app::limits;

pub const ALL_STATUSES: [JobRunStatus; 8] = JobRunStatus::ALL;


#[derive(Deserialize)]
pub struct HomeQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
    pub filter_job_id: Option<String>,
    pub filter_status: Option<JobRunStatus>,
}

impl HomeQuery {
    pub fn page(&self) -> usize {
        self.page.unwrap_or(1).max(1)
    }

    /// The query string's own value, defaulted and capped by `[ui]` in config.toml.
    pub fn page_size(&self, app_config: &AppConfig) -> usize {
        self.page_size
            .unwrap_or(app_config.ui.page_size as usize)
            .clamp(1, app_config.ui.max_page_size as usize)
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

/// The shape the panel template renders. `full` is decided once here (via
/// `limits::is_full`) rather than as a `>=` comparison in the template, the same reason
/// `JobRunDisplay` in the run table precomputes `span_pct` and `status_word` instead of
/// leaving arithmetic to Askama.
pub struct LimitPanelRow {
    pub name: String,
    pub in_use: u32,
    pub max: u32,
    pub full: bool,
}

fn limit_panel_rows(rows: Vec<limits::LimitRow>) -> Vec<LimitPanelRow> {
    rows.iter()
        .map(|row| LimitPanelRow {
            name: row.name.clone(),
            in_use: row.in_use,
            max: row.max,
            full: limits::is_full(row),
        })
        .collect()
}

#[derive(Template)]
#[template(path = "routes/home/route.html")]
struct HomeRouteTemplate {
    current_route: &'static str,
    page_size: usize,
    refresh_seconds: u32,
    table_href: String,
    jobs: Vec<Job>,
    selected_job_id: Option<String>,
    selected_status: Option<JobRunStatus>,
    status_chips: Vec<StatusChip>,
    limit_rows: Vec<LimitPanelRow>,
}

pub async fn home_route(
    Extension(crud): Extension<CRUD>,
    State(state): State<AppState>,
    Query(query): Query<HomeQuery>,
) -> impl IntoResponse {
    let app_config = &state.toolkit.app_config;

    let page = query.page();
    let page_size = query.page_size(app_config);
    let filter_job_id = query.job_id();

    let jobs = crud.select_jobs(&*state.conn_pool, &SelectJobsData {
        filter: SelectJobsDataFilter {
            job_id: None,
            name_like: None,
        },
        sort: Some(SelectJobsDataSort::Alphabetical),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

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

    // A data directory whose database briefly can't be reached shouldn't blank the whole
    // home page - the panel just reads as nothing running, the same defensiveness
    // `select_jobs` above already uses.
    let (running_attempts, claimed_limit_slots) = match state.conn_pool.acquire().await {
        Ok(mut conn) => (
            crud.count_running_attempts(&mut conn).await.unwrap_or_default(),
            crud.claimed_limit_slots(&mut conn).await.unwrap_or_default(),
        ),
        Err(_) => (0, BTreeMap::new()),
    };

    let limit_rows = limit_panel_rows(limits::limit_rows(
        app_config.orchestrator.max_running_attempts,
        running_attempts,
        &app_config.concurrency_limits,
        &claimed_limit_slots,
    ));

    let template = HomeRouteTemplate {
        current_route: "home",
        page_size,
        refresh_seconds: app_config.ui.refresh_interval_seconds,
        table_href,
        jobs,
        selected_job_id: filter_job_id,
        selected_status: query.filter_status,
        status_chips,
        limit_rows,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The boundary a prior review caught explained backwards: `0` is "no ceiling", so a
    /// row configured that way must never render `full`, no matter how much is in use.
    #[test]
    fn a_zero_max_row_never_renders_full() {
        let rows = limit_panel_rows(vec![limits::LimitRow {
            name: "disabled".to_string(),
            in_use: 9,
            max: 0,
        }]);

        assert!(!rows[0].full);
        assert_eq!(rows[0].max, 0);
    }

    #[test]
    fn a_row_at_its_non_zero_max_renders_full() {
        let rows = limit_panel_rows(vec![limits::LimitRow {
            name: "warehouse".to_string(),
            in_use: 3,
            max: 3,
        }]);

        assert!(rows[0].full);
    }

    #[test]
    fn a_row_below_its_non_zero_max_does_not_render_full() {
        let rows = limit_panel_rows(vec![limits::LimitRow {
            name: "openai_api".to_string(),
            in_use: 4,
            max: 5,
        }]);

        assert!(!rows[0].full);
    }
}
