CREATE TABLE job_run_stop (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_run_id INTEGER NOT NULL,
    created_at DATETIME NOT NULL,
    FOREIGN KEY (job_run_id) REFERENCES job_run (id)
);

CREATE INDEX idx_job_run_stop_job_run_id ON job_run_stop (job_run_id);
