CREATE TABLE schedule_job (
    row_id INTEGER NOT NULL,
    schedule_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    parameters TEXT NOT NULL,
    FOREIGN KEY (schedule_id) REFERENCES schedule (schedule_id),
    FOREIGN KEY (job_id) REFERENCES job (job_id),
    UNIQUE (row_id)
);

CREATE INDEX idx_schedule_job_schedule_id ON schedule_job (schedule_id);
CREATE INDEX idx_schedule_job_job_id ON schedule_job (job_id);
