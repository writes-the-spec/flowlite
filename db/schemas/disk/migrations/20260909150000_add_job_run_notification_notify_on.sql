-- A rebuild rather than an ALTER: notify_on is NOT NULL and the schema carries no
-- DEFAULT clauses, so a table that already holds rows cannot have one added in place.
CREATE TABLE job_run_notification_new (
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

-- Every row that exists before this migration was written for an `on_failure:` block,
-- because a failure was the only thing a job could ask to be told about.
INSERT INTO job_run_notification_new (
    id, job_run_id, job_id, notify_on, channel, recipients, status, error, created_at, sent_at
)
SELECT
    id, job_run_id, job_id, 'failure', channel, recipients, status, error, created_at, sent_at
FROM job_run_notification;

DROP TABLE job_run_notification;

ALTER TABLE job_run_notification_new RENAME TO job_run_notification;

CREATE INDEX idx_job_run_notification_job_run_id ON job_run_notification (job_run_id);
CREATE INDEX idx_job_run_notification_status ON job_run_notification (status);
