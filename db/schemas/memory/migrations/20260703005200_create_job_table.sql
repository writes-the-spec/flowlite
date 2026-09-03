CREATE TABLE job (
    row_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,

    PRIMARY KEY (job_id),
    UNIQUE (row_id)
);

CREATE INDEX idx_job_job_id ON job (job_id);