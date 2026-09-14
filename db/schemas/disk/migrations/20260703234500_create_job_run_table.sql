CREATE TABLE job_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    job_description TEXT NOT NULL,
    parameters TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    -- The instant this run is due, which every run has: a manual submission is due when
    -- it was asked for. Not nullable, because "no due time" is not a state a run can be
    -- in - it was the old spelling of "nobody scheduled this", which schedule_id now says.
    scheduled_at DATETIME NOT NULL,
    -- The schedule that asked for this run, or NULL for one nobody scheduled. No foreign
    -- key: `schedule` lives in the `mem` database, which a migration may not reference,
    -- and job_run already denormalises the mem-side job through job_id and job_name
    -- rather than pointing at it.
    schedule_id TEXT,
    started_at DATETIME,
    finished_at DATETIME,
    status TEXT NOT NULL
);

CREATE INDEX idx_job_run_job_id ON job_run (job_id);
CREATE INDEX idx_job_run_created_at ON job_run (created_at);
CREATE INDEX idx_job_run_status ON job_run (status);
-- The Scheduler's reconcile asks this on every pass: what has this schedule already got
-- outstanding, and when is each one due?
CREATE INDEX idx_job_run_schedule_id ON job_run (schedule_id, scheduled_at);
