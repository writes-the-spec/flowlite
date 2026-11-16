-- `deleted` is no longer a job run status. A run removed by hand before it was due ran
-- nothing, which is what `skipped` already says, and its task runs were skipped at the time.
UPDATE job_run SET status = 'skipped' WHERE status = 'deleted';
