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
