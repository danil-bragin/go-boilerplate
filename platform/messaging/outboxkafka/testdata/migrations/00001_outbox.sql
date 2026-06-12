-- +goose Up
-- Partitioned by created_at from day one (ADR-0016); DEFAULT partition keeps
-- "simple" mode behaving like a plain table.
create table outbox (
    id             uuid        not null,
    aggregate_type text        not null,
    aggregate_id   text        not null,
    event_type     text        not null,
    payload        bytea       not null,
    headers        jsonb       not null default '{}'::jsonb,
    created_at     timestamptz not null default now(),
    published_at   timestamptz,
    primary key (id, created_at)
) partition by range (created_at);

create table outbox_default partition of outbox default;

create index outbox_unpublished_idx on outbox (created_at) where published_at is null;

-- +goose Down
drop table outbox;
