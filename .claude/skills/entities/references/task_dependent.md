# `task_dependent` (mem)

The dependency edges of [`task`](task.md), one row per edge: `(job_id, task_id, dependent_task_id)`. No primary key — only `UNIQUE (row_id)` — and foreign keys on `(task_id, job_id)` and `(dependent_task_id, job_id)` back to `task`.

## Written by

`CRUD::init`, from the same source and in the same transaction as `task.depends_on`.

## Read by nothing at all

No code path selects it. It is kept deliberately: it is the shape a reverse-edge query (*"which tasks depend on me?"*) needs, which the JSON column cannot answer without scanning every row.

- **It is not the runtime's dependency source.** `CRUD::submit_job` copies `task.depends_on` onto each `task_run`, and `TaskRunDispatcher::get_dependent_task_runs` resolves that copy. An edge added here changes nothing.
- **Keep it in sync with `task.depends_on`** if you change how dependencies are declared — the two are the same list, stored twice.
