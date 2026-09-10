-- Nullable, and an ALTER rather than the rebuild 20260909150000 used for a NOT NULL
-- column: task_run_attempt and task_run_attempt_output both reference task_run (id), and
-- with PRAGMA foreign_keys on a DROP TABLE performs an implicit delete that any existing
-- attempt row turns into a constraint failure. Every test builds an empty database from
-- these files, so a rebuild here would pass the suite and fail on the first real
-- upgrade. NULL is a genuine state and not a second spelling of empty: a run submitted
-- before this migration could not name a secret.
ALTER TABLE task_run ADD COLUMN secret_env TEXT;
