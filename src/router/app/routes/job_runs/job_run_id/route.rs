use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse, Redirect};
use askama::Template;
use axum::Extension;
use chrono::{DateTime, Utc};
use std::cmp::Ordering;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput};
use crate::crud::task_run::{TaskRun, SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRunStatus};
use crate::router::app::app_state::AppState;
use crate::shared::format;

pub struct JobRunDisplay {
    pub id: i64,
    pub job_id: String,
    pub job_name: String,
    pub job_description: String,
    pub status: JobRunStatus,
    pub status_word: &'static str,
    pub created_at: String,
    pub scheduled_at: String,
    /// Name and value, sorted, for a table rather than a debug-formatted map.
    pub parameters: Vec<(String, String)>,
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
    theme: &'static str,
    job_run: JobRunDisplay,
    lanes: Vec<Lane>,
    ticks: Vec<Tick>,
    task_count: String,
    polling: bool,
    refresh_seconds: u32,
    deletable: bool,
    skippable: bool,
    stoppable: bool,
    rerunnable: bool,
    /// See `JobRunDisplay::job_exists` on the home table: a run can outlive its job, and
    /// one submitted from a file never had one to begin with.
    job_exists: bool,
}

fn idle_label(status: TaskRunStatus) -> &'static str {
    match status {
        TaskRunStatus::Planned => "planned",
        TaskRunStatus::Waiting => "waiting",
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

    let job_run = crud.select_job_run(conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: Some(job_run_id),
            job_id: None,
            status: None,
            statuses: None,
            schedule_id: None,
            scheduled_at: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await.unwrap_or(None);

    let job_run = match job_run {
        Some(job_run) => job_run,
        None => return Html("Job Run not found".to_string()).into_response(),
    };

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
        let depends_on = Some(task_run.depends_on.0.join(", "))
            .filter(|depends_on| !depends_on.is_empty());

        build_lane(task_run, depends_on, window_start, window_end, window_ms)
    }).collect::<Vec<_>>();

    let lane_count = lanes.len();

    let duration = job_run.started_at.map(|started_at| {
        let finished_at = job_run.finished_at.unwrap_or(now);
        format::duration(finished_at.signed_duration_since(started_at).num_seconds())
    });

    let job = crud.select_job(conn, &SelectJobsData {
        filter: SelectJobsDataFilter {
            job_id: Some(job_run.job_id.clone()),
            name_like: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await.unwrap_or_default();

    let controls = job_run_controls(job_run.status);

    let template = JobRunIdRouteTemplate {
        current_route: "home",
        theme: state.toolkit.app_config.ui.theme.as_attribute(),
        job_exists: job.is_some(),
        job_run: JobRunDisplay {
            id: job_run.id,
            job_id: job_run.job_id,
            job_name: job_run.job_name,
            job_description: job_run.job_description,
            status: job_run.status,
            status_word: format::job_run_word(job_run.status),
            created_at: format::timestamp(job_run.created_at),
            scheduled_at: format::timestamp(job_run.scheduled_at),
            parameters: job_run.parameters.0.clone().into_iter().collect(),
            started_at: job_run.started_at.map(format::timestamp),
            finished_at: job_run.finished_at.map(format::timestamp),
            duration,
        },
        lanes,
        ticks: build_ticks(window_seconds),
        task_count: format!("{} task{}", lane_count, if lane_count == 1 { "" } else { "s" }),
        // Scheduled included: a run due immediately still spends one poll pass there, and
        // a page loaded during that pass must keep polling rather than going stale.
        polling: matches!(job_run.status, JobRunStatus::Scheduled | JobRunStatus::Queued | JobRunStatus::Running),
        refresh_seconds: state.toolkit.app_config.ui.refresh_interval_seconds,
        deletable: controls.deletable,
        skippable: controls.skippable,
        stoppable: controls.stoppable,
        rerunnable: controls.rerunnable,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}

pub async fn rerun_job_run_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(job_run_id): Path<i64>,
) -> impl IntoResponse {
    let mut conn = match state.conn_pool.acquire().await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("Error rerunning job run: {}", err);
            return Html("Error rerunning job run").into_response();
        }
    };

    // The button renders only on a rerunnable run, so arriving on one that is not means a
    // page that has gone stale - or a POST that never came from the page at all. Either
    // way the run page the redirect lands on says what the run actually is.
    match crate::shared::job_run::select_job_run(&crud, &mut conn, job_run_id).await {
        Ok(job_run) if !job_run.status.is_rerunnable() => {
            return Redirect::to(&format!("/job-runs/{}", job_run_id)).into_response();
        }
        Ok(_) => {}
        Err(err) => {
            eprintln!("Error rerunning job run: {}", err);
            return Html("Error rerunning job run").into_response();
        }
    }

    let result = crud.rerun_job(&mut conn, job_run_id).await;

    match result {
        Ok(new_job_run_id) => {
            state.signals.publish();
            Redirect::to(&format!("/job-runs/{}", new_job_run_id)).into_response()
        }
        Err(err) => {
            eprintln!("Error rerunning job run: {}", err);
            Html("Error rerunning job run").into_response()
        }
    }
}

pub async fn delete_job_run_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(job_run_id): Path<i64>,
) -> impl IntoResponse {
    let mut conn = match state.conn_pool.acquire().await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("Error deleting job run: {}", err);
            return Html("Error deleting job run").into_response();
        }
    };

    // `delete_job_run` re-checks the status inside its own transaction and leaves anything
    // but a `Scheduled` run alone, so a page that has gone stale - or one whose run came due
    // between the render and the click - redirects to a run that says what actually
    // happened, the way the stop route does, rather than to a dead end.
    match crud.delete_job_run(&mut conn, job_run_id).await {
        Ok(_) => {
            state.signals.publish();
            Redirect::to(&format!("/job-runs/{}", job_run_id)).into_response()
        }
        Err(err) => {
            eprintln!("Error deleting job run: {}", err);
            Html("Error deleting job run").into_response()
        }
    }
}

pub async fn stop_job_run_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(job_run_id): Path<i64>,
) -> impl IntoResponse {
    let job_run = crud.select_job_run(&*state.conn_pool, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: Some(job_run_id),
            job_id: None,
            status: None,
            statuses: None,
            schedule_id: None,
            scheduled_at: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await;

    let job_run = match job_run {
        Ok(Some(job_run)) => job_run,
        Ok(None) => return Html("Job run not found").into_response(),
        Err(err) => {
            eprintln!("Error stopping job run: {}", err);
            return Html("Error stopping job run").into_response();
        }
    };

    // A settled run's stop row would never be read, so none is written. Every button that
    // reaches this route - the stop on a queued or running run, the skip in the delete
    // dialog on a scheduled one - renders only on a run that is still going, so arriving
    // here means a page that has gone stale; not an error worth a dead end, and the run
    // page the redirect lands on already says what actually happened.
    if job_run.status.is_finished() {
        return Redirect::to(&format!("/job-runs/{}", job_run_id)).into_response();
    }

    let result = crud.insert_job_run_stop(&*state.conn_pool, &InsertJobRunStopData {
        input: InsertJobRunStopDataInput {
            job_run_id,
        }
    }).await;

    match result {
        Ok(_) => {
            state.signals.publish();
            Redirect::to(&format!("/job-runs/{}", job_run_id)).into_response()
        }
        Err(err) => {
            eprintln!("Error stopping job run: {}", err);
            Html("Error stopping job run").into_response()
        }
    }
}

/// Which of the four buttons a run's status offers. One function, because "may this be
/// stopped?" and "may this be deleted?" are the same question asked at two ends of a run's
/// life, and answering them in separate places is how a run comes to offer both.
struct JobRunControls {
    deletable: bool,
    skippable: bool,
    stoppable: bool,
    rerunnable: bool,
}

fn job_run_controls(status: JobRunStatus) -> JobRunControls {
    JobRunControls {
        // Nothing of it has run, and removing it hands the occurrence back to the schedule.
        deletable: status == JobRunStatus::Scheduled,
        // The same status as the delete, and the opposite answer to the same question: a
        // skip keeps the occurrence, a delete hands it back. Only a scheduled run can be
        // called off before its schedule has had the chance to submit it again.
        skippable: status == JobRunStatus::Scheduled,
        // Queued too, not only Running: a queued run is due and starts on the next pass,
        // and waiting for it to start before offering the stop is offering it too late.
        stoppable: matches!(status, JobRunStatus::Queued | JobRunStatus::Running),
        // Two halves. A rerun replays the run's own snapshot, which is only an answer once
        // the run is over - a button offered earlier is how the same work comes to run
        // twice. And a `Deleted` run is over but not rerunnable: its schedule writes the
        // occurrence again on its own.
        rerunnable: status.is_finished() && status.is_rerunnable(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scheduled run has no process and no output, and deleting it hands its occurrence
    /// back to the schedule — the one status where that is true.
    #[test]
    fn only_a_scheduled_run_offers_delete() {
        for status in JobRunStatus::ALL {
            assert_eq!(
                job_run_controls(status).deletable,
                status == JobRunStatus::Scheduled,
                "{status}",
            );
        }
    }

    /// A skip and a delete are offered together, on the one status where the occurrence is
    /// still the schedule's to decide — they differ in what they leave behind, not in when
    /// they are reachable.
    #[test]
    fn only_a_scheduled_run_offers_skip() {
        for status in JobRunStatus::ALL {
            assert_eq!(
                job_run_controls(status).skippable,
                status == JobRunStatus::Scheduled,
                "{status}",
            );
        }
    }

    /// Queued as well as Running: a queued run is already due and starts on the next pass,
    /// so the page has to offer the stop before anything of it has run.
    #[test]
    fn a_queued_or_running_run_offers_stop() {
        assert!(job_run_controls(JobRunStatus::Queued).stoppable);
        assert!(job_run_controls(JobRunStatus::Running).stoppable);
    }

    /// Stopping settles the run, so a settled one has nothing left to stop — and a
    /// scheduled one is deleted rather than stopped.
    #[test]
    fn a_settled_or_scheduled_run_offers_no_stop() {
        assert!(!job_run_controls(JobRunStatus::Scheduled).stoppable);

        for status in JobRunStatus::ALL.into_iter().filter(|status| status.is_finished()) {
            assert!(!job_run_controls(status).stoppable, "{status}");
        }
    }

    /// A rerun replays the run's own snapshot, which only says something once the run is
    /// over. Offering it earlier invites two runs of the same work at once.
    #[test]
    fn only_a_finished_run_offers_rerun() {
        for status in JobRunStatus::ALL.into_iter().filter(|status| !status.is_finished()) {
            assert!(!job_run_controls(status).rerunnable, "{status}");
        }
    }

    /// Finished, and still no rerun: the occurrence a deleted run held is the schedule's
    /// again, so the button would submit a second copy of what is already coming back.
    /// The one status where the page's two halves part company.
    #[test]
    fn a_deleted_run_offers_no_rerun_though_it_is_finished() {
        assert!(JobRunStatus::Deleted.is_finished());
        assert!(!job_run_controls(JobRunStatus::Deleted).rerunnable);
    }

    /// Every settled status but that one still offers it.
    #[test]
    fn a_settled_run_that_is_not_deleted_offers_rerun() {
        for status in JobRunStatus::ALL.into_iter().filter(|status| status.is_finished()) {
            assert_eq!(
                job_run_controls(status).rerunnable,
                status != JobRunStatus::Deleted,
                "{status}",
            );
        }
    }
}
