CREATE TABLE job_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    job_description TEXT NOT NULL,
    parameters TEXT NOT NULL,
    on_failure_emails TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    scheduled_at DATETIME,
    started_at DATETIME,
    finished_at DATETIME,
    status TEXT NOT NULL
);

CREATE INDEX idx_job_run_job_id ON job_run (job_id);
CREATE INDEX idx_job_run_created_at ON job_run (created_at);
CREATE INDEX idx_job_run_status ON job_run (status);
