use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;

use crate::crud::CRUD;
use crate::crud::schedule::{SelectSchedulesData, SelectSchedulesDataFilter, SelectSchedulesDataSort};
use crate::crud::schedule_job::{ScheduleJob, SelectScheduleJobsData, SelectScheduleJobsDataFilter, SelectScheduleJobsDataSort};
use crate::router::app::app_state::AppState;
use crate::router::app::format;

pub struct ScheduleDisplay {
    pub schedule_id: String,
    pub name: String,
    pub description: String,
    pub cron: String,
    pub timezone: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub next_run: Option<String>,
    pub disabled: bool,
}

#[derive(Template)]
#[template(path = "routes/schedules/schedule_id/route.html")]
struct ScheduleIdRouteTemplate {
    current_route: &'static str,
    schedule: ScheduleDisplay,
    schedule_jobs: Vec<ScheduleJob>,
    refresh_seconds: u32,
}

pub async fn schedule_id_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(schedule_id): Path<String>,
) -> impl IntoResponse {
    let conn = &*state.conn_pool;

    let schedule = crud.select_schedule(conn, &SelectSchedulesData {
        filter: SelectSchedulesDataFilter {
            schedule_id: Some(schedule_id.clone()),
            name_like: None,
            ..Default::default()
        },
        sort: Some(SelectSchedulesDataSort::RowId),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

    let schedule = match schedule {
        Some(schedule) => schedule,
        None => return Html("Schedule not found".to_string()).into_response(),
    };

    let schedule_jobs = crud.select_schedule_jobs(conn, &SelectScheduleJobsData {
        filter: SelectScheduleJobsDataFilter {
            schedule_id: Some(schedule.schedule_id.clone()),
            job_id: None,
        },
        sort: Some(SelectScheduleJobsDataSort::RowId),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

    let template = ScheduleIdRouteTemplate {
        current_route: "schedules",
        schedule: ScheduleDisplay {
            schedule_id: schedule.schedule_id,
            name: schedule.name,
            description: schedule.description,
            cron: schedule.cron,
            timezone: schedule.timezone,
            start_date: schedule.start_date.map(|date| date.to_string()),
            end_date: schedule.end_date.map(|date| date.to_string()),
            next_run: schedule.next_run.map(format::timestamp),
            disabled: schedule.disabled,
        },
        schedule_jobs,
        refresh_seconds: state.toolkit.app_config.ui.refresh_interval_seconds,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
