# `task_dependent` (mem)

The dependency edges of [`task`](task.md), normalized one row per edge: `(job_id, task_id, dependent_task_id)`. No primary key — only `UNIQUE (row_id)` — and foreign keys on both `(task_id, job_id)` and `(dependent_task_id, job_id)` back to `task`.

## Read by nothing at all

`CRUD::init` writes it and no code path anywhere selects it. It is not dead by accident: it is the normalized form of the same list `task.depends_on` holds as JSON, written from the same source in the same transaction, and it is the shape a reverse-edge query would need — *"which tasks depend on me?"* — which the JSON column cannot answer without scanning every row.

Two consequences worth knowing before touching it:

- **It is not the runtime's dependency source.** `CRUD::submit_job` copies `task.depends_on` onto each `task_run`, and `TaskRunDispatcher::get_dependent_task_runs` resolves *that* copy. Adding an edge here changes nothing about execution.
- **The two must be kept in sync** if you change how dependencies are declared. They are genuinely redundant — same source data, same list, written together — rather than two different concepts.
