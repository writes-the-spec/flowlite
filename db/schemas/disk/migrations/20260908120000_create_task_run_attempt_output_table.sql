CREATE TABLE task_run_attempt_output (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_run_attempt_id INTEGER NOT NULL,
    task_run_id INTEGER NOT NULL,
    job_run_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    stream TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    content TEXT NOT NULL,
    FOREIGN KEY (task_run_attempt_id) REFERENCES task_run_attempt (id),
    FOREIGN KEY (task_run_id) REFERENCES task_run (id),
    FOREIGN KEY (job_run_id) REFERENCES job_run (id)
);

CREATE INDEX idx_task_run_attempt_output_task_run_attempt_id ON task_run_attempt_output (task_run_attempt_id, stream, id);
CREATE INDEX idx_task_run_attempt_output_task_run_id ON task_run_attempt_output (task_run_id, id);
CREATE INDEX idx_task_run_attempt_output_job_run_id ON task_run_attempt_output (job_run_id, id);
CREATE INDEX idx_task_run_attempt_output_job_id ON task_run_attempt_output (job_id);
CREATE INDEX idx_task_run_attempt_output_task_id ON task_run_attempt_output (task_id);
