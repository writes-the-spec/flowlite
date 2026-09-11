CREATE TABLE task_run (
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
    limits TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    started_at DATETIME,
    finished_at DATETIME,
    status TEXT NOT NULL,
    FOREIGN KEY (job_run_id) REFERENCES job_run (id)
);

CREATE UNIQUE INDEX idx_job_run_id_task_id_job_id ON task_run (job_run_id, task_id, job_id);
CREATE INDEX idx_task_run_job_run_id ON task_run (job_run_id);
CREATE INDEX idx_task_run_job_id ON task_run (job_id);
CREATE INDEX idx_task_run_task_id ON task_run (task_id);
CREATE INDEX idx_task_run_status ON task_run (status);
