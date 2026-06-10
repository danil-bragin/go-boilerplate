-- name: InsertPendingOrder :exec
-- Pre-inserts the read-model row at POST time with status 'pending' so an
-- immediate GET returns 200 instead of 404. ON CONFLICT DO NOTHING keeps the
-- insert upsert-safe: a racing OrderCreated/PaymentProcessed projection write
-- (or an idempotent POST retry) must never be downgraded back to 'pending'.
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, $2, $3, $4, 'pending', now())
on conflict (order_id) do nothing;

-- name: UpsertOrderCreated :exec
-- A late/redelivered OrderCreated fills in the order details but must never
-- downgrade a terminal status (paid/payment_failed/payment_timeout) back to
-- 'created' — see the terminal-precedence note below.
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, $2, $3, $4, 'created', now())
on conflict (order_id) do update set
  customer_id  = excluded.customer_id,
  amount_cents = excluded.amount_cents,
  currency     = excluded.currency,
  updated_at   = now(),
  status       = case
                   when orders_read.status in ('paid', 'payment_failed', 'payment_timeout')
                   then orders_read.status
                   else 'created'
                 end;

-- Terminal-status precedence (MarkPaid / MarkPaymentFailed / MarkPaymentTimeout):
-- 'paid', 'payment_failed', and 'payment_timeout' are TERMINAL. Precedence is
-- pending < created < {terminal}; the FIRST terminal event wins and any later
-- terminal event is ignored (no row returned — sqlc.ErrNoRows — and the
-- projection logs a warning). This keeps the projection reorder-safe under
-- at-least-once delivery.
--
-- RETURNING created_at feeds the orders.lifecycle.duration histogram: it is
-- only returned when the terminal write APPLIED, so the projection observes
-- the order's created→terminal latency exactly once per order.

-- name: MarkPaid :one
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, '', 0, '', 'paid', now())
on conflict (order_id) do update set
  status     = 'paid',
  updated_at = now()
where orders_read.status not in ('paid', 'payment_failed', 'payment_timeout')
returning created_at;

-- name: MarkPaymentFailed :one
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, '', 0, '', 'payment_failed', now())
on conflict (order_id) do update set
  status     = 'payment_failed',
  updated_at = now()
where orders_read.status not in ('paid', 'payment_failed', 'payment_timeout')
returning created_at;

-- name: MarkPaymentTimeout :one
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, '', 0, '', 'payment_timeout', now())
on conflict (order_id) do update set
  status     = 'payment_timeout',
  updated_at = now()
where orders_read.status not in ('paid', 'payment_failed', 'payment_timeout')
returning created_at;

-- name: GetOrderView :one
select order_id, customer_id, amount_cents, currency, status, created_at, updated_at
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

-- name: ListOrdersByCustomer :many
-- Ownership-scoped variant of ListOrders: same keyset pagination, restricted
-- to one customer's rows. Used for non-admin principals (customer_id = sub).
select order_id, customer_id, amount_cents, currency, status, created_at, updated_at
from orders_read
where customer_id = sqlc.arg(customer_id)::text
  and (created_at, order_id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_order_id)::uuid)
order by created_at desc, order_id desc
limit sqlc.arg(page_limit)::int;
