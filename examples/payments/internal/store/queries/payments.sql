-- name: InsertPayment :exec
insert into payments (id, order_id, amount, asset, status)
values ($1, $2, $3, $4, $5);

-- name: GetPaymentByOrder :one
select id, order_id, amount, asset, status, created_at
from payments
where order_id = $1
limit 1;
