use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse, Redirect};
use askama::Template;
use axum::{Extension, Form};

use crate::router::app::middlewares::session::SignedIn;
use crate::crud::CRUD;
use crate::declared::{Job, Task};
use crate::router::app::app_state::AppState;
use crate::router::app::routes::job_dag::{self as dag, Dag};
use std::collections::BTreeMap;

/// A job id with nothing behind it. Reachable two ways: a run submitted from a file, whose
/// definition was never installed here, and a job deleted from the project directory while its
/// runs remain. Both leave the id readable everywhere the runs are, so it needs a page.
#[derive(Template)]
#[template(path = "routes/job_missing.html")]
struct JobMissingTemplate {
    current_route: &'static str,
    /// Who is signed in, for the masthead; None when `[auth]` is not configured.
    signed_in_as: Option<String>,
    job_id: String,
}

#[derive(Template)]
#[template(path = "routes/job.html")]
struct JobIdRouteTemplate {
    current_route: &'static str,
    /// Who is signed in, for the masthead; None when `[auth]` is not configured.
    signed_in_as: Option<String>,
    job: Job,
    tasks: Vec<Task>,
    dag: Dag,
    has_dependencies: bool,
}

pub async fn job_id_route(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    signed_in: Option<Extension<SignedIn>>,
) -> impl IntoResponse {
    let job = match state.declared.job(&job_id) {
        Some(job) => job.clone(),
        None => {
            let template = JobMissingTemplate {
                current_route: "jobs",
                signed_in_as: signed_in.map(|Extension(signed_in)| signed_in.subject),
                job_id,
            };

            return match template.render() {
                Ok(html) => Html(html).into_response(),
                Err(err) => {
                    eprintln!("Template rendering error: {}", err);
                    Html("Error rendering template".to_string()).into_response()
                },
            };
        },
    };

    let tasks: Vec<Task> = state.declared
        .tasks_of(&job.job_id)
        .into_iter()
        .cloned()
        .collect();

    let dag = dag::build(&tasks);

    let template = JobIdRouteTemplate {
        current_route: "jobs",
        signed_in_as: signed_in.map(|Extension(signed_in)| signed_in.subject),
        job,
        tasks,
        // A graph of unconnected boxes says nothing the task table doesn't already say.
        has_dependencies: !dag.edges.is_empty(),
        dag,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}


/// Submits a run of this job from its page, the browser equivalent of
/// `flowlite job submit <job_id> --param name=value`.
///
/// The form posts one field per parameter the job declares, so `overrides` arrives already
/// shaped like the map `submit_job` takes. A job declaring none posts an empty body and
/// gets an empty map, which is the same submit the button made before it had a form.
///
/// Only values are editable - the names come from the job's own `parameters:`, rendered as
/// the field names - so a viewer cannot invent a name `submit_job` would reject. A crafted
/// POST still can, and that is left to `submit_job`'s own validation rather than
/// re-checked here: it is the same check the CLI's `--param` goes through, and duplicating
/// it would give two places to disagree about what a parameter is.
///
/// Taking a form means the request needs a form content type, which a browser always sends
/// and a bare `curl -X POST` with no body does not - that now answers 415 where it used to
/// submit. The CLI is the supported scriptable path, so this is a deliberate narrowing of
/// the endpoint to what the page posts rather than an accident.
///
/// Unlike stop and rerun, this is not idempotent - every submit is another run - so the
/// page asks for confirmation first, the same way rerun does. What a stray confirmation
/// costs is one queued run: `max_parallel_runs` and the global attempt cap decide what
/// actually executes, so an extra submit waits rather than piling on.
pub async fn submit_job_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(job_id): Path<String>,
    // Last, because it consumes the request body.
    Form(overrides): Form<BTreeMap<String, String>>,
) -> impl IntoResponse {

    let mut conn = match state.conn_pool.acquire().await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("Error submitting job: {}", err);
            return Html("Error submitting job").into_response();
        }
    };

    match crud.submit_job(&mut conn, &state.declared, &job_id, &overrides, chrono::Utc::now(), None, None).await {
        Ok(job_run_id) => {
            state.signals.publish();
            Redirect::to(&format!("/job-runs/{}", job_run_id)).into_response()
        }
        Err(err) => {
            eprintln!("Error submitting job: {}", err);
            Html("Error submitting job").into_response()
        }
    }
}
