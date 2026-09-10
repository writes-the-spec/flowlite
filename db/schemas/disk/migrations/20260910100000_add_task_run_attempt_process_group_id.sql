-- A plain ALTER rather than a rebuild: the column is nullable, so no DEFAULT is needed to
-- fill it in for the rows that already exist. An attempt that never spawned has no process
-- group, and every row written before this migration has no record of the one it had.
--
-- The group id is the spawned child's pid, since the command is spawned with
-- process_group(0). It is on the row because TaskRunAttemptChildren is memory: after a
-- restart this is the only way back to a process that is still running.
ALTER TABLE task_run_attempt ADD COLUMN process_group_id INTEGER;
