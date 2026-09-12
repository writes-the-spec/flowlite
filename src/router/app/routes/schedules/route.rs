use axum::extract::{State, Query};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;
use serde::Deserialize;

use crate::crud::CRUD;
use crate::crud::schedule::{SelectSchedulesData, SelectSchedulesDataFilter, SelectSchedulesDataSort};
use crate::router::app::app_state::AppState;
use crate::shared::format;

pub struct ScheduleEntry {
    pub schedule_id: String,
    pub name: String,
    pub description: String,
    pub cron: String,
    pub next_run: Option<String>,
    pub disabled: bool,
}

#[derive(Template)]
#[template(path = "routes/schedules/route.html")]
struct SchedulesRouteTemplate {
    current_route: &'static str,
    schedules: Vec<ScheduleEntry>,
    current_name: Option<String>,
    prev_href: Option<String>,
    next_href: Option<String>,
    refresh_seconds: u32,
    /// This page's own URL, search and page included, so the poll re-asks the same
    /// question rather than resetting to an unfiltered first page under the reader.
    self_href: String,
}

#[derive(Deserialize)]
pub struct SchedulesRouteQuery {
    pub schedule_name_like: Option<String>,
    pub page: Option<u32>,
}

fn schedules_href(page: u32, name_like: Option<&str>) -> String {
    match name_like {
        Some(name) => format!("/schedules?page={}&schedule_name_like={}", page, name),
        None => format!("/schedules?page={}", page),
    }
}

pub async fn schedules_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Query(query): Query<SchedulesRouteQuery>,
) -> impl IntoResponse {
    let conn = &*state.conn_pool;

    let page_size = state.toolkit.app_config.ui.page_size;

    let page = query.page.unwrap_or(1).max(1);
    let offset = (page - 1) * page_size;

    // A cleared search box posts an empty value, which means no filter rather than a
    // filter on the empty name.
    let name_like = query.schedule_name_like.filter(|name| !name.is_empty());

    let schedules = crud.select_schedules(conn, &SelectSchedulesData {
        filter: SelectSchedulesDataFilter {
            schedule_id: None,
            name_like: name_like.clone(),
            ..Default::default()
        },
        sort: Some(SelectSchedulesDataSort::RowId),
        limit: Some(page_size + 1), // One extra row tells us whether a next page exists.
        offset: Some(offset),
    }).await.unwrap_or_default();

    let has_next_page = schedules.len() > page_size as usize;

    let entries = schedules.into_iter().take(page_size as usize).map(|schedule| {
        ScheduleEntry {
            schedule_id: schedule.schedule_id,
            name: schedule.name,
            description: schedule.description,
            cron: schedule.cron,
            next_run: schedule.next_run.map(format::timestamp),
            disabled: schedule.disabled,
        }
    }).collect();

    let prev_href = if page > 1 {
        Some(schedules_href(page - 1, name_like.as_deref()))
    } else {
        None
    };

    let next_href = if has_next_page {
        Some(schedules_href(page + 1, name_like.as_deref()))
    } else {
        None
    };

    let self_href = schedules_href(page, name_like.as_deref());

    let template = SchedulesRouteTemplate {
        current_route: "schedules",
        schedules: entries,
        current_name: name_like,
        prev_href,
        next_href,
        refresh_seconds: state.toolkit.app_config.ui.refresh_interval_seconds,
        self_href,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
