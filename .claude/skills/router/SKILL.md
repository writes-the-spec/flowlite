---
name: router
description: Add or modify pages in src/router/ - the axum + askama + htmx dashboard that `flowlite serve` binds to localhost, and the templates under templates/routes/ that mirror it. Use when adding a route or an htmx partial, changing what a page shows, wiring a form that POSTs, working out how a handler reaches CRUD or why a page renders "Error rendering template", or deciding where a page's formatting should happen.
---

# Router module conventions (src/router/)

`flowlite serve` builds this and binds it to localhost. [src/router/app/app.rs](../../../src/router/app/app.rs)'s `create_router` lists every route in one place; each handler lives in its own `route.rs` under `src/router/app/routes/`, and its askama template mirrors that path under `templates/routes/`.

```
src/router/app/routes/jobs/job_id/route.rs   ->  templates/routes/jobs/job_id/route.html
```

`src/router/api/` is an empty stub, waiting for a caller that wants HTTP for its own sake. The CLI and the MCP server are not that caller - both open the SQLite files directly.

## Adding a page (e.g. `/widgets`)

1. Create `src/router/app/routes/widgets/route.rs`:

```rust
use askama::Template;
use axum::Extension;
use axum::extract::State;
use axum::response::{Html, IntoResponse};

use crate::crud::CRUD;
use crate::crud::widget::{SelectWidgetsData, SelectWidgetsDataFilter, SelectWidgetsDataSort};
use crate::router::app::app_state::AppState;
use crate::shared::format;

/// What the template renders: the row's fields already turned into the strings the page
/// shows, so the template chooses nothing.
pub struct WidgetDisplay {
    pub widget_id: String,
    pub created_at: String,
}

#[derive(Template)]
#[template(path = "routes/widgets/route.html")]
struct WidgetsRouteTemplate {
    current_route: &'static str,
    widgets: Vec<WidgetDisplay>,
}

pub async fn widgets_route(
    State(state): State<AppState>,
    Extension(crud): Extension<CRUD>,
) -> impl IntoResponse {
    let widgets = crud.select_widgets(&*state.conn_pool, &SelectWidgetsData {
        filter: SelectWidgetsDataFilter { widget_id: None },
        sort: Some(SelectWidgetsDataSort::Alphabetical),
        limit: None,
        offset: None,
    }).await.unwrap_or_default();

    let widgets = widgets
        .into_iter()
        .map(|widget| WidgetDisplay {
            widget_id: widget.widget_id,
            created_at: format::timestamp(widget.created_at),
        })
        .collect();

    let template = WidgetsRouteTemplate {
        current_route: "widgets",
        widgets,
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

2. Add `pub mod route;` to `src/router/app/routes/widgets/mod.rs` and `pub mod widgets;` to [routes/mod.rs](../../../src/router/app/routes/mod.rs). Every directory in the tree is a `mod.rs` of nothing but `pub mod` lines.

3. Register it in [create_router](../../../src/router/app/app.rs), in `main_routes`:

```rust
.route("/widgets", get(app::routes::widgets::route::widgets_route))
```

4. Create `templates/routes/widgets/route.html`:

```html
{% extends "routes/root.html" %}

{% block title %}Widgets · flowlite{% endblock %}

{% block content %}
<table class="runs">
    {% for widget in widgets %}
    <tr><td>{{ widget.widget_id }}</td><td>{{ widget.created_at }}</td></tr>
    {% endfor %}
</table>
{% endblock %}
```

5. If the page belongs in the masthead, add a link to [templates/routes/root.html](../../../templates/routes/root.html) keyed on `current_route`.

## htmx partials

A region that refreshes itself is its own route returning a fragment: [job_run_table](../../../src/router/app/routes/home/job_run_table/route.rs) and [limits_panel](../../../src/router/app/routes/home/limits_panel/route.rs), both fetched by the home page with `hx-get` and `hx-trigger="load, every {{ refresh_seconds }}s"`. A partial's template has no `{% extends %}` and its handler passes no `current_route` - it is a fragment, not a page. `refresh_seconds` comes from `[ui]` in config.toml, through `state.toolkit.app_config.ui.refresh_interval_seconds`.

A partial that shares query parameters with its page imports them rather than redeclaring them: `job_run_table` takes the home page's own `HomeQuery`, `Pagination` and `runs_href`.

## Rules

- **A handler never returns `Result`.** It logs to stderr and renders something: a fallback string for a render failure, a "not found" template for a missing row (see `JobMissingTemplate` in [jobs/job_id/route.rs](../../../src/router/app/routes/jobs/job_id/route.rs)), `.unwrap_or_default()` for a query whose emptiness the page can show. A dashboard that 500s mid-refresh is worse than one showing an empty table.
- **Formatting happens in Rust, not in the template.** A handler builds a `*Display` struct of finished strings using [src/shared/format.rs](../../../src/shared/format.rs), and the template interpolates them. A template may branch on a boolean or an `Option` the handler already decided (`run.job_exists`, `chip.selected`, `row.full`), but it does no arithmetic and no formatting: there is not one askama filter in `templates/`, and a date, a duration or a status word reaching a page unformatted means the work was left in the wrong file.
- **`CRUD` arrives as `Extension`, not built in the handler.** [crud_middleware](../../../src/router/app/middlewares/crud.rs) puts one in every request's extensions; take `Extension(crud): Extension<CRUD>`. Queries run against `&*state.conn_pool`, never a connection of the handler's own.
- **`serve`'s pool attaches `mem`, so one pool reaches both databases.** Jobs and schedules are `mem` tables and are still queried through `conn_pool` - a route never calls `with_fresh_mem`, which is the MCP server's path. `AppState.memory_conn` is held and never read on purpose: dropping it would drop the shared-cache in-memory database out from under the pool. That is the dead-code warning the build prints.
- **A route that changes something publishes a signal and redirects.** `state.signals.publish()` wakes the orchestrator's pollers, then `Redirect::to(...)` sends the browser to the page that shows the outcome, so a refresh does not re-post. See `stop_job_run_route` and `rerun_job_run_route` in [job_runs/job_run_id/route.rs](../../../src/router/app/routes/job_runs/job_run_id/route.rs).
- **Every state-changing method is guarded by [same_origin_middleware](../../../src/router/app/middlewares/same_origin.rs),** which sits outside `crud_middleware` so a refused request opens no connection. The dashboard has no auth and binds to localhost by design; that check is what keeps another site from using the operator's browser as a deputy. A new POST needs nothing added - it is covered by being a POST.
- **Assets are embedded, not served from disk.** [assets.rs](../../../src/router/app/assets.rs) uses `rust_embed` over `assets/`, so a release binary carries its own CSS and vendored htmx/alpine.
- **Never import from `src/cli/` or `src/mcp/`.** Anything the dashboard shares with another frontend - `format`, `limits` - lives in `src/shared/`. See the `frontends` skill; `tests/frontend_boundaries.rs` enforces it.
- **Handlers are not unit-tested; the pure functions beside them are.** A handler needs a request, a pool and a rendered page to exercise, and asserting on HTML is brittle. So a decision worth testing is lifted out into a plain function that takes what it needs and returns a value - `is_allowed` in [same_origin.rs](../../../src/router/app/middlewares/same_origin.rs), `limit_panel_rows` in [limits_panel/route.rs](../../../src/router/app/routes/home/limits_panel/route.rs) - and that function gets the `#[cfg(test)] mod tests`. `tests/serve_lock.rs` and `tests/serve_secret_check.rs` cover the serving process around all of it.
