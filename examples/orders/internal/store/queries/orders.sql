-- name: InsertOrder :exec
insert into orders (id, customer_id, amount_cents, currency, status)
values ($1, $2, $3, $4, $5);

-- name: GetOrder :one
select id, customer_id, amount_cents, currency, status, created_at
from orders
where id = $1;
