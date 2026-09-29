---
name: cli
description: Add or modify CLI commands in src/cli/ (clap-based subcommands). Use when adding a new top-level command, a new subcommand under an existing one, or changing how a command talks to Toolkit/CRUD.
---

# CLI module conventions (src/cli/)

`clap` derive. `src/cli/cli.rs` holds the root `Cli` and the `Command` enum; each command is one file under `src/cli/commands/`.

## Adding a top-level command (e.g. `widget`)

1. Create `src/cli/commands/widget.rs`:

```rust
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
pub struct WidgetListCmd {}

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

        let widgets = crud.select_widgets(&mut conn, &SelectWidgetsData { /* filter, sort, limit, offset */ }).await?;

        if json {
            println!("{}", serde_json::to_string_pretty(&widgets)?);
            return Ok(());
        }

        // ... the human table, or "No widgets found"
        Ok(())
    }
}
```

2. Add `pub mod widget;` to [src/cli/commands/mod.rs](../../../src/cli/commands/mod.rs).
3. In [src/cli/cli.rs](../../../src/cli/cli.rs), add `WidgetCmd` to `Command` and the arm `Command::Widget(cmd) => cmd.run(toolkit).await?,` in `Cli::run`.

## Rules

- **One file per top-level command**, named after it, singular (`job.rs`, `serve.rs`).
- **Shape:** with subcommands, `<Name>Cmd` (holding `#[command(subcommand)] pub command`) + `<Name>Subcommand` enum + one `<Name><Action>Cmd` each, as in [src/cli/commands/job.rs](../../../src/cli/commands/job.rs). Without (`serve`, `limits`), one flat `#[derive(Args)]` struct.
- **Every `run` takes `toolkit: Toolkit` by value and returns `anyhow::Result<()>`**; a leaf that prints rows also takes `json: bool` from its group.
- **`--json` is declared once per top-level command.** A group declares it `global = true`, so it parses after the subcommand (`flowlite job-run get 7 --json`); a flat command (`limits`, `status`) uses plain `#[arg(long)]` and reads `self.json`. Not on the root `Cli`, which would force it onto `serve`. Each leaf branches inline, not via a shared output type: list and detail tables are different shapes.
- **JSON on stdout is the rows themselves, no `{ok, data}` envelope; errors never go there.** Errors stay `anyhow::Result`, which `main` prints to stderr before exiting 1, so a redirected `--json` file holds rows or nothing. A command reporting an outcome (`job submit --wait`) must not also print it to stdout, or the failure is stated twice.
- **No imports from `src/mcp/` or `src/router/`** except `serve` and `mcp` starting them ([tests/frontend_boundaries.rs](../../../tests/frontend_boundaries.rs) allows those by name). Shared helpers (`format`, `limits`, `JobRunDetail`, `wait_for_job_run`, `parse_job_run_status`) live in `src/shared/`; see the `frontends` skill.
- **Data goes through `CRUD::new(Arc::new(toolkit))`** and the `crud::<entity>` query structs (`crud` skill); no raw SQL here.
- **Only a command that reads config seeds it.** One querying a `mem` table holds a `toolkit.get_memory_conn()` binding for the whole command (it runs the memory migrations and keeps the shared-cache database alive) and calls `crud.init(&mut conn)` first, like `job list` and `job submit`. One reading only run history does neither: the `job-run` commands ([src/cli/commands/job_run.rs](../../../src/cli/commands/job_run.rs)), `logs` and `rerun` included, read no config so they still work when a YAML file in the data dir no longer parses.
- **Long-running commands** (`serve`) build `Toolkit`/`CRUD`/the pool as `Arc`s - follow [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs). Services that move runs are wired in `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)); a service the orchestrator neither calls nor is called by (`Scheduler`, `NotificationService`, `RetentionService`) gets its own `Poller` in `serve.rs`, its wake-up registered before any poller spawns (`poller` skill).
