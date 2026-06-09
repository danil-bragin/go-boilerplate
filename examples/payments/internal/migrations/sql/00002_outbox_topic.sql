-- +goose Up
-- Explicit destination topic per outbox row (mirrors platform outbox
-- migration 00003): aggregate_type returns to being the aggregate kind.
alter table outbox add column topic text;
update outbox set topic = aggregate_type where topic is null;
-- (applied migration; table was tiny at backfill time — the full-scan lock
-- window the adding-not-nullable-field rule guards against did not apply)
-- squawk-ignore adding-not-nullable-field
alter table outbox alter column topic set not null;

-- +goose Down
alter table outbox drop column topic;
