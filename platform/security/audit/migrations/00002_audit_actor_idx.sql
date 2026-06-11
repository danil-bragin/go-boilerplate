-- +goose Up
-- Backs the DSAR/audit read path (PgStore.Query / QueryByActor): one actor's
-- trail filtered by created_at, newest first.
--
-- NOTE: plain CREATE INDEX takes a write lock for the duration of the build,
-- which is fine for the boilerplate default (new/small tables). For an
-- already-large production audit_log, create the index CONCURRENTLY instead:
-- CONCURRENTLY cannot run inside a transaction, so such a migration must be
-- marked with the goose "NO TRANSACTION" annotation.
create index audit_log_actor_idx on audit_log (actor, created_at);

-- +goose Down
drop index audit_log_actor_idx;
