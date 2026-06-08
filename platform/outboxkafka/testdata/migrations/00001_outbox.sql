-- +goose Up
create table outbox (
    id             uuid primary key,
    aggregate_type text        not null,
    aggregate_id   text        not null,
    event_type     text        not null,
    payload        bytea       not null,
    headers        jsonb       not null default '{}'::jsonb,
    created_at     timestamptz not null default now(),
    published_at   timestamptz
);

create index outbox_unpublished_idx on outbox (created_at) where published_at is null;

-- +goose Down
drop table outbox;
