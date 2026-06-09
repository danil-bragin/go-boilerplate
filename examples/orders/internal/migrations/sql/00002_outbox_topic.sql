-- +goose Up
-- Explicit destination topic per outbox row (mirrors platform outbox
-- migration 00003): aggregate_type returns to being the aggregate kind.
alter table outbox add column topic text;
update outbox set topic = aggregate_type where topic is null;
alter table outbox alter column topic set not null;

-- +goose Down
alter table outbox drop column topic;
