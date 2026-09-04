use axum::{Router, routing::{get, post}, middleware};
use crate::router::app::assets::static_handler;
use crate::router::app;


use crate::router::app::app_state::AppState;


pub fn create_router(app_state: AppState) -> Router {
    let main_routes = Router::new()
        .route("/", get(app::routes::home::route::home_route))
        .route("/job-run-table", get(app::routes::home::job_run_table::route::job_run_table_route))
        .route("/jobs", get(app::routes::jobs::route::jobs_route))
        .route("/jobs/{job_id}", get(app::routes::jobs::job_id::route::job_id_route))
        .route("/schedules", get(app::routes::schedules::route::schedules_route))
        .route("/schedules/{schedule_id}", get(app::routes::schedules::schedule_id::route::schedule_id_route))
        .route("/job-runs/{job_run_id}", get(app::routes::job_runs::job_run_id::route::job_run_id_route))
        .route("/job-runs/{job_run_id}/stop", post(app::routes::job_runs::job_run_id::route::stop_job_run_route))
        .route("/job-runs/{job_run_id}/rerun", post(app::routes::job_runs::job_run_id::route::rerun_job_run_route))
        .route("/task-runs/{task_run_id}", get(app::routes::task_runs::task_run_id::route::task_run_id_route));

    Router::new()
        .merge(main_routes)
        .route("/assets/{*path}", get(static_handler))
        .route_layer(middleware::from_fn_with_state(app_state.clone(), app::middlewares::crud::crud_middleware))
        .with_state(app_state)
}
