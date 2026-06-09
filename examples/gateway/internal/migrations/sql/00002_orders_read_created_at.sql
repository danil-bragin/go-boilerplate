-- +goose Up
-- created_at backs the keyset pagination cursor of GET /v1/orders
-- (ordered by (created_at, order_id) descending).
alter table orders_read add column created_at timestamptz not null default now();
create index orders_read_created_at_idx on orders_read (created_at desc, order_id desc);

-- +goose Down
drop index orders_read_created_at_idx;
alter table orders_read drop column created_at;
