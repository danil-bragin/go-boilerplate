-- name: InsertPendingOrder :exec
-- Pre-inserts the read-model row at POST time with status 'pending' so an
-- immediate GET returns 200 instead of 404. ON CONFLICT DO NOTHING keeps the
-- insert upsert-safe: a racing OrderCreated/PaymentProcessed projection write
-- (or an idempotent POST retry) must never be downgraded back to 'pending'.
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, $2, $3, $4, 'pending', now())
on conflict (order_id) do nothing;

-- name: UpsertOrderCreated :exec
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, $2, $3, $4, 'created', now())
on conflict (order_id) do update set
  customer_id  = excluded.customer_id,
  amount_cents = excluded.amount_cents,
  currency     = excluded.currency,
  updated_at   = now(),
  status       = case when orders_read.status = 'paid' then 'paid' else 'created' end;

-- name: MarkPaid :exec
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, '', 0, '', 'paid', now())
on conflict (order_id) do update set
  status     = 'paid',
  updated_at = now();

-- name: GetOrderView :one
select order_id, customer_id, amount_cents, currency, status, updated_at
from orders_read
where order_id = $1;

-- name: ListOrders :many
-- Keyset pagination, newest first. The caller passes the (created_at,
-- order_id) of the last row of the previous page; the first page passes
-- (timestamptz 'infinity', max uuid) so every row qualifies.
select order_id, customer_id, amount_cents, currency, status, created_at, updated_at
from orders_read
where (created_at, order_id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_order_id)::uuid)
order by created_at desc, order_id desc
limit sqlc.arg(page_limit)::int;
