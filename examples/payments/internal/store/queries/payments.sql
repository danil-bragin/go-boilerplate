-- name: InsertPayment :exec
insert into payments (id, order_id, amount_cents, status)
values ($1, $2, $3, $4);

-- name: GetPaymentByOrder :one
select id, order_id, amount_cents, status, created_at
from payments
where order_id = $1
limit 1;
