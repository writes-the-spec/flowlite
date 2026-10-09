use axum::{Router, routing::{get, post}, middleware};
use crate::router::app::assets::static_handler;
use crate::router::app;


use crate::router::app::app_state::AppState;


pub fn create_router(app_state: AppState) -> Router {
    let main_routes = Router::new()
        .route("/", get(app::routes::home::route::home_route))
        .route("/job-run-table", get(app::routes::home::job_run_table::route::job_run_table_route))
        .route("/limits-panel", get(app::routes::home::limits_panel::route::limits_panel_route))
        .route("/jobs", get(app::routes::jobs::route::jobs_route))
        .route("/jobs/{job_id}", get(app::routes::jobs::job_id::route::job_id_route))
        .route("/jobs/{job_id}/submit", post(app::routes::jobs::job_id::route::submit_job_route))
        .route("/jobs/{job_id}/tasks/{task_id}", get(app::routes::jobs::job_id::task_id::route::task_id_route))
        .route("/schedules", get(app::routes::schedules::route::schedules_route))
        .route("/schedules/{schedule_id}", get(app::routes::schedules::schedule_id::route::schedule_id_route))
        .route("/job-runs/{job_run_id}", get(app::routes::job_runs::job_run_id::route::job_run_id_route))
        .route("/job-runs/{job_run_id}/stop", post(app::routes::job_runs::job_run_id::route::stop_job_run_route))
        .route("/job-runs/{job_run_id}/rerun", post(app::routes::job_runs::job_run_id::route::rerun_job_run_route))
        .route("/job-runs/{job_run_id}/delete", post(app::routes::job_runs::job_run_id::route::delete_job_run_route))
        .route("/task-runs/{task_run_id}", get(app::routes::task_runs::task_run_id::route::task_run_id_route))
        // On these routes only: the assets and the login form are how someone without a
        // session gets one. It lets everything through when there is no `[auth]`.
        .route_layer(middleware::from_fn_with_state(app_state.clone(), app::middlewares::session::session_middleware));

    let mut router = Router::new()
        .merge(main_routes)
        .route("/assets/{*path}", get(static_handler));

    // Without `[auth]` there is nothing to log in to, so /login is a 404 like any path
    // this server does not have.
    if app_state.auth.is_some() {
        router = router
            .route("/login", get(app::routes::login::route::login_page_route).post(app::routes::login::route::login_route))
            .route("/logout", post(app::routes::logout::route::logout_route));
    }

    router
        .route_layer(middleware::from_fn_with_state(app_state.clone(), app::middlewares::crud::crud_middleware))
        // Outside crud_middleware: a refused request should not reach a handler or open a
        // connection, and this decides on headers alone.
        .route_layer(middleware::from_fn(app::middlewares::same_origin::same_origin_middleware))
        .with_state(app_state)
}
