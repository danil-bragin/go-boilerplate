-- +goose Up
-- Partial index for retention cleanup: DeletePublishedBefore filters on
-- "published_at is not null and published_at < $1", which previously forced
-- an hourly full-table scan. The partial predicate keeps the index small
-- (unpublished rows — the hot set — are excluded) and also serves the
-- outbox.pending backlog count cheaply via the complementary unpublished
-- partial index from 00001.
--
-- NOTE: plain CREATE INDEX takes a write lock for the duration of the build,
-- which is fine for the boilerplate default (new/small tables). For an
-- already-large production outbox, create the index CONCURRENTLY instead:
-- CONCURRENTLY cannot run inside a transaction, so such a migration must be
-- marked with the goose "NO TRANSACTION" annotation.
create index outbox_published_at_idx on outbox (published_at) where published_at is not null;

-- +goose Down
drop index outbox_published_at_idx;
