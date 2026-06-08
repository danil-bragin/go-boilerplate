-- name: UpsertOrderCreated :exec
insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at)
values ($1, $2, $3, $4, 'created', now())
on conflict (order_id) do update
    set customer_id  = excluded.customer_id,
        amount_cents = excluded.amount_cents,
        currency     = excluded.currency,
        status       = excluded.status,
        updated_at   = excluded.updated_at;

-- name: MarkPaid :exec
update orders_read
set status     = 'paid',
    updated_at = now()
where order_id = $1;

-- name: GetOrderView :one
select order_id, customer_id, amount_cents, currency, status, updated_at
from orders_read
where order_id = $1;
