-- +goose Up
-- High-volume, low-value demo write (A2): one INSERT, NO outbox — so the
-- RecordProductView command can run with ConsistencyPolicy Eventual +
-- Transactional:false (no wrapping tx, async best-effort audit). Contrast with
-- the order flow, which is Strong/effectively-once via the Kafka outbox.
create table product_views (
    id          bigserial   primary key,
    product_id  text        not null,
    viewer      text        not null,
    viewed_at   timestamptz not null default now()
);

-- +goose Down
drop table product_views;
