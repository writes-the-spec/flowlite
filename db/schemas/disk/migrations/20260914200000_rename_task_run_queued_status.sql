-- The task run's single `queued` status became two: `planned` while its job run has yet
-- to start, and `waiting` once JobRunDispatcher has released it. Nothing else wrote
-- `queued`, so every row holding it is in flight and belongs on one side or the other.
UPDATE task_run
SET status = 'waiting'
WHERE status = 'queued'
  AND job_run_id IN (SELECT id FROM job_run WHERE status = 'running');

UPDATE task_run
SET status = 'planned'
WHERE status = 'queued';
