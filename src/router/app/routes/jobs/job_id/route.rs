use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;

use crate::crud::CRUD;
use crate::crud::job::{Job, SelectJobsData, SelectJobsDataFilter, SelectJobsDataSort};
use crate::crud::task::{Task, SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};
use crate::router::app::app_state::AppState;
use crate::router::app::routes::jobs::job_id::dag::{self, Dag};

#[derive(Template)]
#[template(path = "routes/jobs/job_id/route.html")]
struct JobIdRouteTemplate {
    current_route: &'static str,
    job: Job,
    tasks: Vec<Task>,
    dag: Dag,
    has_dependencies: bool,
}

pub async fn job_id_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let conn = &*state.conn_pool;

    let job = crud.select_job(conn, &SelectJobsData {
        filter: SelectJobsDataFilter {
            job_id: Some(job_id.clone()),
            name_like: None,
        },
        sort: Some(SelectJobsDataSort::RowId),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

    let job = match job {
        Some(job) => job,
        None => return Html("Job not found".to_string()).into_response(),
    };

    let tasks = crud.select_tasks(conn, &SelectTasksData {
        filter: SelectTasksDataFilter {
            task_id: None,
            job_id: Some(job.job_id.clone()),
        },
        sort: Some(SelectTasksDataSort::RowId),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

    let dag = dag::build(&tasks);

    let template = JobIdRouteTemplate {
        current_route: "jobs",
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
