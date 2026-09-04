CREATE TABLE task (
    row_id INTEGER NOT NULL,
    task_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    command TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    timeout INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    retry_delay INTEGER NOT NULL,
    PRIMARY KEY (task_id, job_id),
    FOREIGN KEY (job_id) REFERENCES job (job_id),
    UNIQUE (row_id)
);

CREATE INDEX idx_task_job_id ON task (job_id);
CREATE INDEX idx_task_task_id ON task (task_id);
