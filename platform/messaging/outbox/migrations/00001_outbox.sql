-- +goose Up
-- Outbox is RANGE-partitioned by created_at from day one (ADR-0016). A
-- partitioned table's primary key must include the partition key, hence
-- (id, created_at). created_at is immutable, so a row never moves partitions
-- (unlike published_at, which is set after insert).
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

-- DEFAULT partition. In "simple" mode (OUTBOX_PARTITION_MODE=simple, the
-- default) every row lands here and the outbox behaves exactly like a plain
-- table — age-based DeletePublishedBefore is the cleanup mechanism. In
-- "partitioned" mode the opt-in maintenance worker pre-creates time-range
-- partitions ahead of now (so DEFAULT stays empty in steady state) and
-- DETACH+DROPs expired ones. See ADR-0016.
create table outbox_default partition of outbox default;

-- Partial index for the relay's unpublished scan; propagated to every
-- partition automatically.
create index outbox_unpublished_idx on outbox (created_at) where published_at is null;

-- +goose Down
drop table outbox;
