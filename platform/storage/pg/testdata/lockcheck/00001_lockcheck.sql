-- +goose Up
-- Verifies that the migration statements execute on the SAME session that
-- holds the pg.Migrate advisory lock (key 0x67626f696c65 = 113672473635941:
-- classid = high 32 bits = 26466, objid = low 32 bits = 1869180005).
-- If goose ran on a different connection than the lock holder, this raises
-- and the migration fails.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_locks
        WHERE locktype = 'advisory'
          AND pid = pg_backend_pid()
          AND classid = 26466
          AND objid = 1869180005
          AND granted
    ) THEN
        RAISE EXCEPTION 'migration session does not hold the pg.Migrate advisory lock';
    END IF;
END
$$;
-- +goose StatementEnd

CREATE TABLE lockcheck (id int PRIMARY KEY);

-- +goose Down
DROP TABLE lockcheck;
