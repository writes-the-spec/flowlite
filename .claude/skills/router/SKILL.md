---
name: router
description: Add or modify pages in src/router/ - the axum + askama + htmx dashboard that `flowlite serve` binds to localhost, and the templates under templates/routes/ that mirror it. Use when adding a route or an htmx partial, changing what a page shows, wiring a form that POSTs, working out how a handler reaches CRUD or why a page renders "Error rendering template", or deciding where a page's formatting should happen.
---

# Router module conventions (src/router/)

[src/router/app/app.rs](../../../src/router/app/app.rs)'s `create_router` lists every route. Each handler is a `route.rs` under `src/router/app/routes/`; its askama template mirrors the path (`routes/jobs/job_id/route.rs` -> `templates/routes/jobs/job_id/route.html`). `src/router/api/` is an empty stub; the CLI and MCP server open the SQLite files directly, not over HTTP.

## Adding a page (e.g. `/widgets`)

1. Create `src/router/app/routes/widgets/route.rs`:

```rust
pub struct WidgetDisplay {          // finished strings: the template chooses nothing
    pub widget_id: String,
    pub created_at: String,
}

#[derive(Template)]
#[template(path = "routes/widgets/route.html")]
struct WidgetsRouteTemplate {
    current_route: &'static str,
    theme: &'static str,
    widgets: Vec<WidgetDisplay>,
}

pub async fn widgets_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
) -> impl IntoResponse {
    let widgets = crud.select_widgets(&*state.conn_pool, &SelectWidgetsData { /* ... */ })
        .await.unwrap_or_default();

    let template = WidgetsRouteTemplate {
        current_route: "widgets",
        theme: state.toolkit.app_config.ui.theme.as_attribute(),
        widgets: widgets.into_iter().map(|w| WidgetDisplay {
            widget_id: w.widget_id,
            created_at: format::timestamp(w.created_at),   // crate::shared::format
        }).collect(),
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            eprintln!("Template rendering error: {}", err);
            Html("Error rendering template".to_string()).into_response()
        },
    }
}
```

2. Add `pub mod route;` to `routes/widgets/mod.rs` and `pub mod widgets;` to [routes/mod.rs](../../../src/router/app/routes/mod.rs); every `mod.rs` in the tree is only `pub mod` lines.
3. In [create_router](../../../src/router/app/app.rs)'s `main_routes`: `.route("/widgets", get(app::routes::widgets::route::widgets_route))`
4. Create `templates/routes/widgets/route.html` with `{% extends "routes/root.html" %}`, a `{% block title %}Widgets · flowlite{% endblock %}` and a `{% block content %}`.
5. For a masthead link, edit [templates/routes/root.html](../../../templates/routes/root.html), keyed on `current_route`.

Every page struct carries `current_route` and `theme`. `root.html` renders `theme` into `<html data-theme="...">`, which the light palette keys on, so a page without it does not compile; it comes from `[ui] theme`. "Error rendering template" on a page means the `Err` arm above ran; the askama error is on serve's stderr.

## htmx partials

A self-refreshing region is its own route returning a fragment - [job_run_table](../../../src/router/app/routes/home/job_run_table/route.rs), [limits_panel](../../../src/router/app/routes/home/limits_panel/route.rs) - fetched by the home page with `hx-get` and `hx-trigger="load, every {{ refresh_seconds }}s"` (`refresh_seconds` = `app_config.ui.refresh_interval_seconds`). A partial's template has no `{% extends %}`; its handler passes no `current_route` or `theme`. A partial sharing query parameters with its page imports them (`job_run_table` uses the home page's `HomeQuery`, `Pagination`, `runs_href`).

## Rules

- **A handler never returns `Result`**; it logs to stderr and renders something - a fallback string, a "not found" template (`JobMissingTemplate` in [jobs/job_id/route.rs](../../../src/router/app/routes/jobs/job_id/route.rs)), `.unwrap_or_default()` - since a dashboard that 500s mid-refresh is worse than an empty table.
- **Formatting happens in Rust.** The handler builds a `*Display` of finished strings with [src/shared/format.rs](../../../src/shared/format.rs). A template may branch on a bool or `Option` the handler decided (`run.job_exists`, `chip.selected`, `row.full`) but does no arithmetic or formatting - `templates/` has no askama filter.
- **`CRUD` arrives as `Extension<CRUD>`** from [crud_middleware](../../../src/router/app/middlewares/crud.rs); queries run on `&*state.conn_pool`, never a connection of the handler's own.
- **`serve`'s pool attaches `mem`**, so `mem` tables (jobs, schedules) are queried through `conn_pool` too; a route never calls `with_fresh_mem` (that is MCP's path). `AppState.memory_conn` is held and never read on purpose: dropping it drops the shared-cache in-memory database and the pool attaches an empty one. Its `#[allow(dead_code)]` keeps the build warning-free, so a new dead-code warning means something really is unused.
- **A state-changing route publishes a signal and redirects:** `state.signals.publish()` wakes the pollers, then `Redirect::to(...)` the page showing the outcome, so a refresh does not re-post. See `stop_job_run_route` / `rerun_job_run_route` in [job_runs/job_run_id/route.rs](../../../src/router/app/routes/job_runs/job_run_id/route.rs).
- **[same_origin_middleware](../../../src/router/app/middlewares/same_origin.rs) guards every state-changing method,** layered outside `crud_middleware` so a refused request opens no connection. With no auth by design, this is what stops another site using the operator's browser as a deputy. A new POST is covered automatically.
- **Assets are embedded:** [assets.rs](../../../src/router/app/assets.rs) uses `rust_embed` over `assets/`, so a release binary carries its CSS and vendored htmx.
- **Never import from `src/cli/` or `src/mcp/`;** shared needs (`format`, `limits`) live in `src/shared/`. See the `frontends` skill; `tests/frontend_boundaries.rs` enforces it.
- **Handlers are not unit-tested; the pure functions beside them are.** Lift a decision into a plain function and test that - `is_allowed` in [same_origin.rs](../../../src/router/app/middlewares/same_origin.rs), `limit_panel_rows` in [limits_panel/route.rs](../../../src/router/app/routes/home/limits_panel/route.rs). `tests/serve_lock.rs` and `tests/serve_secret_check.rs` cover the serving process.
