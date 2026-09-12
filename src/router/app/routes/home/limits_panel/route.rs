use std::collections::BTreeMap;

use askama::Template;
use axum::Extension;
use axum::extract::State;
use axum::response::{Html, IntoResponse};

use crate::crud::CRUD;
use crate::router::app::app_state::AppState;
use crate::shared::limits;

/// The shape the panel template renders. `full` is decided once here (via
/// `limits::is_full`) rather than as a `>=` comparison in the template, the same reason
/// `JobRunDisplay` in the run table precomputes `span_pct` and `status_word` instead of
/// leaving arithmetic to Askama.
pub struct LimitPanelRow {
    pub name: String,
    pub in_use: u32,
    pub max: u32,
    pub full: bool,
}

fn limit_panel_rows(rows: Vec<limits::LimitRow>) -> Vec<LimitPanelRow> {
    rows.iter()
        .map(|row| LimitPanelRow {
            name: row.name.clone(),
            in_use: row.in_use,
            max: row.max,
            full: limits::is_full(row),
        })
        .collect()
}

#[derive(Template)]
#[template(path = "routes/home/limits_panel/route.html")]
struct LimitsPanelTemplate {
    limit_rows: Vec<LimitPanelRow>,
}

/// The limits panel as its own fragment, so the home page can poll it the way it polls the
/// run table.
///
/// It has to refresh: `in use` is live state, and the panel exists to answer "why is
/// nothing running". Rendered once with the page it would freeze beside a run table that
/// keeps moving, and a reader has no cue which of the two is stale — worse than not
/// showing it, since a stale zero reads as "nothing is holding a slot".
pub async fn limits_panel_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
) -> impl IntoResponse {

    let app_config = &state.toolkit.app_config;

    // A data directory whose database briefly can't be reached shouldn't blank the panel -
    // it reads as nothing running, the same defensiveness the home page's own queries use.
    let (running_attempts, claimed_limit_slots) = match state.conn_pool.acquire().await {
        Ok(mut conn) => (
            crud.count_running_attempts(&mut conn).await.unwrap_or_default(),
            crud.claimed_limit_slots(&mut conn).await.unwrap_or_default(),
        ),
        Err(_) => (0, BTreeMap::new()),
    };

    let template = LimitsPanelTemplate {
        limit_rows: limit_panel_rows(limits::limit_rows(
            app_config.orchestrator.max_running_attempts,
            running_attempts,
            &app_config.concurrency_limits,
            &claimed_limit_slots,
        )),
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The boundary a prior review caught explained backwards: `0` is "no ceiling", so a
    /// row configured that way must never render `full`, no matter how much is in use.
    #[test]
    fn a_zero_max_row_never_renders_full() {
        let rows = limit_panel_rows(vec![limits::LimitRow {
            name: "disabled".to_string(),
            in_use: 9,
            max: 0,
        }]);

        assert!(!rows[0].full);
        assert_eq!(rows[0].max, 0);
    }

    #[test]
    fn a_row_at_its_non_zero_max_renders_full() {
        let rows = limit_panel_rows(vec![limits::LimitRow {
            name: "warehouse".to_string(),
            in_use: 3,
            max: 3,
        }]);

        assert!(rows[0].full);
    }

    #[test]
    fn a_row_below_its_non_zero_max_does_not_render_full() {
        let rows = limit_panel_rows(vec![limits::LimitRow {
            name: "openai_api".to_string(),
            in_use: 4,
            max: 5,
        }]);

        assert!(!rows[0].full);
    }
}
