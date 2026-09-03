CREATE TABLE task_dependent (
    row_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    dependent_task_id TEXT NOT NULL,
    FOREIGN KEY (task_id, job_id) REFERENCES task (task_id, job_id),
    FOREIGN KEY (dependent_task_id, job_id) REFERENCES task (task_id, job_id),
    UNIQUE (row_id)
);

CREATE INDEX idx_task_dependent_job_id ON task_dependent (job_id);
CREATE INDEX idx_task_dependent_task_id ON task_dependent (task_id);
CREATE INDEX idx_task_dependent_dependent_task_id ON task_dependent (dependent_task_id);
