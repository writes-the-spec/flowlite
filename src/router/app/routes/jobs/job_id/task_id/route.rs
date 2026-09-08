use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter, SelectJobsDataSort};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};
use crate::router::app::app_state::AppState;

/// The task as declared, name and value pairs already sorted for a table rather than a
/// debug-formatted map.
pub struct TaskDisplay {
    pub task_id: String,
    pub job_id: String,
    pub description: String,
    pub command: String,
    pub depends_on: Vec<String>,
    pub timeout: u32,
    pub max_retries: u32,
    pub retry_delay: u32,
    pub env: Vec<(String, String)>,
    pub working_dir: String,
}

#[derive(Template)]
#[template(path = "routes/jobs/job_id/task_id/route.html")]
struct TaskIdRouteTemplate {
    current_route: &'static str,
    job_name: String,
    task: TaskDisplay,
}

pub async fn task_id_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
    Path((job_id, task_id)): Path<(String, String)>,
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

    // Both ids, because a task id is unique per job rather than globally.
    let task = crud.select_task(conn, &SelectTasksData {
        filter: SelectTasksDataFilter {
            task_id: Some(task_id),
            job_id: Some(job.job_id.clone()),
        },
        sort: Some(SelectTasksDataSort::RowId),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

    let task = match task {
        Some(task) => task,
        None => return Html("Task not found".to_string()).into_response(),
    };

    let template = TaskIdRouteTemplate {
        current_route: "jobs",
        job_name: job.name,
        task: TaskDisplay {
            task_id: task.task_id,
            job_id: task.job_id,
            description: task.description,
            command: task.command,
            depends_on: task.depends_on.0,
            timeout: task.timeout,
            max_retries: task.max_retries,
            retry_delay: task.retry_delay,
            env: task.env.0.into_iter().collect(),
            working_dir: task.working_dir,
        },
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
