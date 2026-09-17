CREATE TABLE task_run_attempt (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_run_id INTEGER NOT NULL,
    job_run_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    started_at DATETIME,
    finished_at DATETIME,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    -- What the command wrote to $FLOWLITE_TASK_OUTPUT, empty for nothing at all - the
    -- convention task_run.stdin uses for the other end of the same channel. Bounded by
    -- [orchestrator] max_task_output_bytes, past which the attempt fails instead, so this
    -- never holds a truncated result. Per attempt rather than per task run because the
    -- file it is read from is per attempt: a retry must not inherit the bytes of the
    -- attempt it replaces, and a failed attempt's result is what item 13 will hand the
    -- attempt after it.
    output TEXT NOT NULL,
    -- Nullable: an attempt that never spawned has no process group. The id is the
    -- spawned child's pid, since the command is spawned with process_group(0). It is on
    -- the row because TaskRunAttemptChildren is memory: after a restart this is the only
    -- way back to a process that is still running.
    process_group_id INTEGER,
    FOREIGN KEY (task_run_id) REFERENCES task_run (id),
    FOREIGN KEY (job_run_id) REFERENCES job_run (id)
);

CREATE UNIQUE INDEX idx_task_run_attempt_task_run_id_attempt ON task_run_attempt (task_run_id, attempt);
CREATE INDEX idx_task_run_attempt_task_run_id ON task_run_attempt (task_run_id);
CREATE INDEX idx_task_run_attempt_job_run_id ON task_run_attempt (job_run_id);
CREATE INDEX idx_task_run_attempt_job_id ON task_run_attempt (job_id);
CREATE INDEX idx_task_run_attempt_task_id ON task_run_attempt (task_id);
CREATE INDEX idx_task_run_attempt_status ON task_run_attempt (status);
