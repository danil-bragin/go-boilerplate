-- +goose Up
-- Explicit destination topic for each outbox row. Historically the topic was
-- smuggled through aggregate_type; rows enqueued before this migration are
-- backfilled with that value so the relay keeps publishing them to the same
-- topic, and aggregate_type can return to being the real aggregate kind.
alter table outbox add column topic text;
update outbox set topic = aggregate_type where topic is null;
alter table outbox alter column topic set not null;

-- +goose Down
alter table outbox drop column topic;
