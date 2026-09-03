CREATE TABLE schedule (
    row_id INTEGER NOT NULL,
    schedule_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    cron TEXT NOT NULL,
    timezone TEXT NOT NULL,
    start_date DATE,
    end_date DATE,
    disabled INTEGER NOT NULL,
    next_run TIMESTAMP,
    PRIMARY KEY (schedule_id),
    UNIQUE (row_id)
);

CREATE INDEX idx_schedule_schedule_id ON schedule (schedule_id);
