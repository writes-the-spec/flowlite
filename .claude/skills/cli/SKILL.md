---
name: cli
description: Add or modify CLI commands in src/cli/ (clap-based subcommands). Use when adding a new top-level command, a new subcommand under an existing one, or changing how a command talks to Toolkit/CRUD.
---

# CLI module conventions (src/cli/)

Commands are built with `clap` derive macros. `src/cli/cli.rs` defines the root `Cli` struct and top-level `Command` enum; each command's implementation lives in its own file under `src/cli/commands/`.

## Adding a new top-level command (e.g. `widget`)

1. Create `src/cli/commands/widget.rs`:

```rust
use clap::{Args, Subcommand};
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::crud::widget::{SelectWidgetsData, SelectWidgetsDataFilter};

#[derive(Args)]
pub struct WidgetCmd {
    #[command(subcommand)]
    pub command: WidgetSubcommand,

    /// Print the result as JSON instead of as text.
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand)]
pub enum WidgetSubcommand {
    List(WidgetListCmd),
}

#[derive(Args)]
pub struct WidgetListCmd {
}

impl WidgetCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {
        let _memory_conn = toolkit.get_memory_conn().await?;

        match &self.command {
            WidgetSubcommand::List(cmd) => cmd.run(toolkit, self.json).await,
        }
    }
}

impl WidgetListCmd {
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {
        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));
        crud.init(&mut conn).await?;

        let widgets = crud.select_widgets(&mut conn, &SelectWidgetsData {
            filter: SelectWidgetsDataFilter { widget_id: None },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

        if json {
            println!("{}", serde_json::to_string_pretty(&widgets)?);
            return Ok(());
        }

        if widgets.is_empty() {
            println!("No widgets found");
        } else {
            println!("{:<20}", "Widget ID");
            println!("{}", "-".repeat(20));
            for widget in widgets {
                println!("{:<20}", widget.widget_id);
            }
        }

        Ok(())
    }
}
```

2. Register it in [src/cli/commands/mod.rs](../../../src/cli/commands/mod.rs):

```rust
pub mod widget;
```

3. Wire it into [src/cli/cli.rs](../../../src/cli/cli.rs): add `WidgetCmd` to the `Command` enum, `use` its module, and add a match arm in `Cli::run`:

```rust
Command::Widget(cmd) => cmd.run(toolkit).await?,
```

## Rules

- One file per top-level command under `src/cli/commands/`, named after the command (singular, e.g. `job.rs`, `serve.rs`).
- A command with subcommands follows the `<Name>Cmd` (has `#[command(subcommand)] pub command: <Name>Subcommand`) + `<Name>Subcommand` enum + one `<Name><Action>Cmd` struct per subcommand pattern (see [src/cli/commands/job.rs](../../../src/cli/commands/job.rs)). A command with no subcommands (like `serve`) is just a flat `#[derive(Args)]` struct with an `impl <Name>Cmd { pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> }`.
- Every `run` takes `toolkit: Toolkit` by value (not a reference) and returns `anyhow::Result<()>`. A command that prints rows also takes `json: bool` after it, passed down by the group command that declares the flag.
- **`--json` is declared once per top-level command, with `global = true`,** so it parses after the subcommand the way a user types it (`flowlite job-run get 7 --json`) and every subcommand under it accepts it. It is not on the root `Cli`, which would force the flag through `serve` as well. Each leaf then branches on it in three lines - serialize the rows and return - rather than through a shared output type: the human table for a list and the one for a detail page are different shapes, and a trait unifying them would have to be opened to understand either call site.
- **JSON goes to stdout as the rows themselves, with no `{ok, data}` envelope, and errors never go there.** An error stays an `anyhow::Result`, which `main` prints to stderr before exiting 1 - so the exit code carries the failure and a redirected `--json` file holds rows or nothing. A command that reports an outcome (`job submit --wait`) must not also print that outcome to stdout, or the failure is stated twice.
- Data-accessing commands build a `CRUD::new(Arc::new(toolkit))` from the connection and use the `crud::<entity>` query structs (see the `crud` skill) — don't write raw SQL in CLI code.
- **Only a command that reads config seeds it.** If the command queries a `mem` table, hold a `toolkit.get_memory_conn()` binding for the lifetime of the command — it runs the memory migrations that create the `mem` tables and keeps the shared-cache database alive — and call `crud.init(&mut conn)` before querying, as the template above and `job list`/`job submit` do. A command that reads only run history does neither: `job-run logs` and `job-run rerun` ([src/cli/commands/job_run.rs](../../../src/cli/commands/job_run.rs)) read no config on purpose, so they still work when a YAML file in the data dir no longer parses.
- Register the command struct in `Command` (in [src/cli/cli.rs](../../../src/cli/cli.rs)) and add the corresponding `mod` line to [src/cli/commands/mod.rs](../../../src/cli/commands/mod.rs).
- Long-running commands (like `serve`) instead construct the shared `Toolkit`/`CRUD`/connection pool as `Arc`s and hand them to the relevant types — follow [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs) for that shape. The background services are not constructed there one by one: `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)) owns that wiring, so a new poller is added to the orchestrator, not to the command.
