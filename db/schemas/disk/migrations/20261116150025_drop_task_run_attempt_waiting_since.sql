-- An attempt no longer records that it is waiting on another run: one asleep in a wait
-- holds its concurrency slot like any other Running attempt.
ALTER TABLE task_run_attempt DROP COLUMN waiting_since;
