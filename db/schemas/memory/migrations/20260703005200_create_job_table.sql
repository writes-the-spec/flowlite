CREATE TABLE job (
    row_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    max_parallel_runs INTEGER NOT NULL,
    parameters TEXT NOT NULL,
    env TEXT NOT NULL,
    secret_env TEXT NOT NULL,
    on_failure_recipients TEXT NOT NULL,
    on_success_recipients TEXT NOT NULL,
    limits TEXT NOT NULL,

    PRIMARY KEY (job_id),
    UNIQUE (row_id)
);

CREATE INDEX idx_job_job_id ON job (job_id);