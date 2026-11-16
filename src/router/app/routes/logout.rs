use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::http::header::SET_COOKIE;
use axum::response::{IntoResponse, Redirect, Response};

use crate::router::app::app_state::AppState;


pub async fn logout_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {

    let Some(auth) = state.auth.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    if let Some(session_id) = auth.cookie.read(&headers) {
        auth.sessions.end(&session_id);
    }

    (
        [(SET_COOKIE, auth.cookie.clear())],
        Redirect::to("/login"),
    ).into_response()
}
