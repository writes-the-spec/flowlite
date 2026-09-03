use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use crate::router::app::app_state::AppState;
use crate::crud::CRUD;

pub async fn crud_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let crud = CRUD::new(state.toolkit.clone());
    req.extensions_mut().insert(crud);
    next.run(req).await
}
