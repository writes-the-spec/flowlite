use axum::extract::{State, Path};
use axum::response::{Html, IntoResponse};
use askama::Template;
use axum::Extension;

use crate::router::app::middlewares::session::SignedIn;
use crate::router::app::app_state::AppState;

/// The task as declared, name and value pairs already sorted for a table rather than a
/// debug-formatted map.
pub struct TaskDisplay {
    pub task_id: String,
    pub job_id: String,
    pub description: String,
    pub command: String,
    pub stdin: String,
    pub depends_on: Vec<String>,
    pub timeout: u32,
    pub idle_timeout: u32,
    pub max_retries: u32,
    pub retry_delay: u32,
    pub env: Vec<(String, String)>,
    pub secret_env: Vec<(String, String)>,
    pub working_dir: String,
}

#[derive(Template)]
#[template(path = "routes/task.html")]
struct TaskIdRouteTemplate {
    current_route: &'static str,
    theme: &'static str,
    /// Who is signed in, for the masthead; None when `[auth]` is not configured.
    signed_in_as: Option<String>,
    job_name: String,
    task: TaskDisplay,
}

pub async fn task_id_route(
    State(state): State<AppState>,
    Path((job_id, task_id)): Path<(String, String)>,
    signed_in: Option<Extension<SignedIn>>,
) -> impl IntoResponse {
    let Some(job) = state.declared.job(&job_id) else {
        return Html("Job not found".to_string()).into_response();
    };

    // Both ids, because a task id is unique per job rather than globally.
    let Some(task) = state.declared.task(&job_id, &task_id) else {
        return Html("Task not found".to_string()).into_response();
    };

    let template = TaskIdRouteTemplate {
        current_route: "jobs",
        theme: state.toolkit.app_config.ui.theme.as_attribute(),
        signed_in_as: signed_in.map(|Extension(signed_in)| signed_in.subject),
        job_name: job.name.clone(),
        task: TaskDisplay {
            task_id: task.task_id.clone(),
            job_id: task.job_id.clone(),
            description: task.description.clone(),
            command: task.command.clone(),
            stdin: task.stdin.clone(),
            depends_on: task.depends_on.clone(),
            timeout: task.timeout,
            idle_timeout: task.idle_timeout,
            max_retries: task.max_retries,
            retry_delay: task.retry_delay,
            env: task.env.clone().into_iter().collect(),
            secret_env: task.secret_env.clone().into_iter().collect(),
            working_dir: task.working_dir.clone(),
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
