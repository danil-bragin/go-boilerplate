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
