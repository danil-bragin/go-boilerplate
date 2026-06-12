-- +goose Up
create table orders (
    id           uuid        primary key,
    customer_id  text        not null,
    amount_cents bigint      not null,
    currency     text        not null,
    status       text        not null,
    created_at   timestamptz not null default now()
);

-- Partitioned by created_at from day one (ADR-0016); the DEFAULT partition
-- keeps "simple" mode behaving exactly like a plain table.
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

create table inbox (
    consumer     text        not null,
    message_id   text        not null,
    processed_at timestamptz not null default now(),
    primary key (consumer, message_id)
);

create table audit_log (
    id         bigserial   primary key,
    actor      text        not null,
    action     text        not null,
    subject    text        not null,
    metadata   jsonb       not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index audit_log_action_idx on audit_log (action, created_at);

-- +goose Down
drop table audit_log;
drop table inbox;
drop index outbox_unpublished_idx;
drop table outbox;
drop table orders;
