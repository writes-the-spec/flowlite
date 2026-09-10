-- no-transaction
-- A rebuild rather than an ALTER: secret_env is NOT NULL and the schema carries no DEFAULT
-- clause, so a table that already holds rows cannot have it added in place. task_run_attempt
-- and task_run_attempt_output both reference task_run (id), so with foreign_keys ON the
-- DROP TABLE below performs an implicit delete that any existing attempt row turns into a
-- constraint failure - confirmed against a copy of a real database with 2741 such rows.
-- PRAGMA defer_foreign_keys does NOT rescue this: the deferred check counts unresolved
-- violations and renaming a table into place never decrements that counter, so the COMMIT
-- fails too. Hence foreign_keys=OFF, which cannot be set inside a transaction, which is why
-- this migration declares -- no-transaction and manages its own transaction below, so a
-- crash mid-migration rolls back cleanly rather than stranding a half-built database.
-- '{}' is what every row that predates this column gets: no run submitted before this
-- migration could name a secret, so an empty map is the truthful value, not a placeholder.
PRAGMA foreign_keys=OFF;
BEGIN;

CREATE TABLE task_run_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_run_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    command TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    timeout INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    retry_delay INTEGER NOT NULL,
    env TEXT NOT NULL,
    secret_env TEXT NOT NULL,
    working_dir TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    started_at DATETIME,
    finished_at DATETIME,
    status TEXT NOT NULL,
    FOREIGN KEY (job_run_id) REFERENCES job_run (id)
);

INSERT INTO task_run_new (
    id, job_run_id, job_id, task_id, command, depends_on, timeout, max_retries, retry_delay,
    env, secret_env, working_dir, created_at, started_at, finished_at, status
)
SELECT
    id, job_run_id, job_id, task_id, command, depends_on, timeout, max_retries, retry_delay,
    env, '{}', working_dir, created_at, started_at, finished_at, status
FROM task_run;

DROP TABLE task_run;

ALTER TABLE task_run_new RENAME TO task_run;

CREATE UNIQUE INDEX idx_job_run_id_task_id_job_id ON task_run (job_run_id, task_id, job_id);
CREATE INDEX idx_task_run_job_run_id ON task_run (job_run_id);
CREATE INDEX idx_task_run_job_id ON task_run (job_id);
CREATE INDEX idx_task_run_task_id ON task_run (task_id);
CREATE INDEX idx_task_run_status ON task_run (status);

COMMIT;
PRAGMA foreign_keys=ON;
