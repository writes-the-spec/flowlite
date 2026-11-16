use axum::{Router, routing::{get, post}, middleware};
use crate::router::app::assets::static_handler;
use crate::router::app;


use crate::router::app::app_state::AppState;


pub fn create_router(app_state: AppState) -> Router {
    let main_routes = Router::new()
        .route("/", get(app::routes::home::home_route))
        .route("/job-run-table", get(app::routes::job_run_table::job_run_table_route))
        .route("/limits-panel", get(app::routes::limits_panel::limits_panel_route))
        .route("/jobs", get(app::routes::jobs::jobs_route))
        .route("/jobs/{job_id}", get(app::routes::job::job_id_route))
        .route("/jobs/{job_id}/submit", post(app::routes::job::submit_job_route))
        .route("/jobs/{job_id}/tasks/{task_id}", get(app::routes::task::task_id_route))
        .route("/schedules", get(app::routes::schedules::schedules_route))
        .route("/schedules/{schedule_id}", get(app::routes::schedule::schedule_id_route))
        .route("/job-runs/{job_run_id}", get(app::routes::job_run::job_run_id_route))
        .route("/job-runs/{job_run_id}/stop", post(app::routes::job_run::stop_job_run_route))
        .route("/job-runs/{job_run_id}/rerun", post(app::routes::job_run::rerun_job_run_route))
        .route("/task-runs/{task_run_id}", get(app::routes::task_run::task_run_id_route))
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
            .route("/login", get(app::routes::login::login_page_route).post(app::routes::login::login_route))
            .route("/logout", post(app::routes::logout::logout_route));
    }

    router
        .route_layer(middleware::from_fn_with_state(app_state.clone(), app::middlewares::crud::crud_middleware))
        // Outside crud_middleware: a refused request should not reach a handler or open a
        // connection, and this decides on headers alone.
        .route_layer(middleware::from_fn(app::middlewares::same_origin::same_origin_middleware))
        // Outermost, and on every route including the reads: a rebinding page reads
        // responses, so a GET is as much its target as a POST.
        .route_layer(middleware::from_fn_with_state(app_state.clone(), app::middlewares::loopback_host::loopback_host_middleware))
        .with_state(app_state)
}
