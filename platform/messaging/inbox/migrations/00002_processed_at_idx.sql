-- +goose Up
-- Index for retention cleanup: the inbox cleaner deletes on
-- "processed_at < $1", which previously forced an hourly full-table scan
-- over the whole dedup table. processed_at is NOT NULL (default now()), so
-- a plain b-tree covers every row.
--
-- NOTE: plain CREATE INDEX takes a write lock for the duration of the build,
-- which is fine for the boilerplate default (new/small tables). For an
-- already-large production inbox, create the index CONCURRENTLY instead:
-- CONCURRENTLY cannot run inside a transaction, so such a migration must be
-- marked with the goose "NO TRANSACTION" annotation.
create index inbox_processed_at_idx on inbox (processed_at);

-- +goose Down
drop index inbox_processed_at_idx;
