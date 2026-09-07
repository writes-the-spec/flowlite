CREATE TABLE task_run_attempt_output (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_run_attempt_id INTEGER NOT NULL,
    stream TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    content TEXT NOT NULL,
    FOREIGN KEY (task_run_attempt_id) REFERENCES task_run_attempt (id)
);

CREATE INDEX idx_task_run_attempt_output_task_run_attempt_id ON task_run_attempt_output (task_run_attempt_id, stream, id);
