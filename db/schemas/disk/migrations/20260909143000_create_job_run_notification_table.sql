CREATE TABLE job_run_notification (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_run_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    notify_on TEXT NOT NULL,
    channel TEXT NOT NULL,
    recipients TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    sent_at DATETIME,
    FOREIGN KEY (job_run_id) REFERENCES job_run (id)
);

CREATE INDEX idx_job_run_notification_job_run_id ON job_run_notification (job_run_id);
CREATE INDEX idx_job_run_notification_status ON job_run_notification (status);
