use askama::Template;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::http::header::SET_COOKIE;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::Form;
use serde::Deserialize;

use crate::auth::{IdpError, SessionCheck};
use crate::router::app::app_state::AppState;
use crate::router::app::middlewares::session::safe_next;


#[derive(Template)]
#[template(path = "routes/login/route.html")]
struct LoginRouteTemplate {
    current_route: &'static str,
    theme: &'static str,
    signed_in_as: Option<String>,
    next: String,
    username: String,
    error: Option<&'static str>,
}

#[derive(Deserialize)]
pub struct LoginQuery {
    next: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    username: String,
    password: String,
    next: Option<String>,
}

pub async fn login_page_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<LoginQuery>,
) -> Response {

    let Some(auth) = state.auth.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let next = safe_next(query.next.as_deref().unwrap_or("/")).to_string();

    if let Some(session_id) = auth.cookie.read(&headers) {
        let check = auth.sessions.check(&session_id, &auth.idp, state.toolkit.get_current_ts()).await;

        if let SessionCheck::SignedIn(_) = check {
            return Redirect::to(&next).into_response();
        }
    }

    render(&state, StatusCode::OK, next, String::new(), None)
}

pub async fn login_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> Response {

    let Some(auth) = state.auth.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let next = safe_next(form.next.as_deref().unwrap_or("/")).to_string();
    let now = state.toolkit.get_current_ts();

    match auth.idp.password(&form.username, &form.password, now).await {
        Ok(grant) => {
            eprintln!("Signed {} in to the dashboard.", form.username);

            // Whatever session the browser arrived with is not carried over: the login
            // always gets an id of its own.
            if let Some(previous) = auth.cookie.read(&headers) {
                auth.sessions.end(&previous);
            }

            let session_id = auth.sessions.start(form.username, grant, now);

            (
                [(SET_COOKIE, auth.cookie.set(&session_id))],
                Redirect::to(&next),
            ).into_response()
        }
        Err(IdpError::Rejected(reason)) => {
            eprintln!("Refused a dashboard login for {}: {reason}.", form.username);

            render(&state, StatusCode::UNAUTHORIZED, next, form.username, Some("Wrong username or password."))
        }
        Err(error @ IdpError::Unavailable(_)) => {
            eprintln!("Could not log {} in to the dashboard: {error}.", form.username);

            render(
                &state,
                StatusCode::SERVICE_UNAVAILABLE,
                next,
                form.username,
                Some("The identity provider cannot be reached. Try again in a moment."),
            )
        }
    }
}

/// The form, again: the username is kept so a mistyped password costs one field, and the
/// password never is.
fn render(
    state: &AppState,
    status: StatusCode,
    next: String,
    username: String,
    error: Option<&'static str>,
) -> Response {

    let template = LoginRouteTemplate {
        current_route: "login",
        theme: state.toolkit.app_config.ui.theme.as_attribute(),
        signed_in_as: None,
        next,
        username,
        error,
    };

    match template.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
