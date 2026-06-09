-- +goose Up
-- payment_timeout_emitted guards the unpaid-order watcher: the flag is set in
-- the SAME transaction that enqueues the OrderPaymentTimedOut outbox row, so
-- the event is emitted at most once per order no matter how often the watcher
-- re-polls (or how many instances run concurrently — the guarded UPDATE makes
-- the emit a compare-and-set).
alter table orders add column payment_timeout_emitted boolean not null default false;

create index orders_unpaid_idx on orders (created_at)
    where status = 'created' and payment_timeout_emitted = false;

-- +goose Down
drop index orders_unpaid_idx;
alter table orders drop column payment_timeout_emitted;
