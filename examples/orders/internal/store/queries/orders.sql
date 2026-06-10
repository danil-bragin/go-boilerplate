-- name: InsertOrder :exec
insert into orders (id, customer_id, amount_cents, currency, status)
values ($1, $2, $3, $4, $5);

-- name: GetOrder :one
select id, customer_id, amount_cents, currency, status, created_at
from orders
where id = $1;

-- name: MarkOrderPaymentOutcome :execrows
-- Records the terminal payment outcome ('paid' or 'payment_failed') on the
-- order row. Only transitions from 'created' — a second (conflicting or
-- duplicate) outcome is ignored, keeping the transition reorder-safe under
-- at-least-once delivery.
update orders
set status = sqlc.arg(status)::text
where id = $1 and status = 'created';

-- name: ListUnpaidExpired :many
-- Orders past the payment deadline that have not had a timeout emitted yet.
-- The cutoff is computed against the DB clock (now()) so multiple instances
-- agree on expiry regardless of app-host clock skew.
select id, created_at
from orders
where status = 'created'
  and payment_timeout_emitted = false
  and created_at < now() - make_interval(secs => sqlc.arg(deadline_seconds)::float8)
order by created_at
limit sqlc.arg(batch_limit)::int;

-- name: MarkPaymentTimeoutEmitted :execrows
-- Compare-and-set guard for the watcher: 0 rows means another poll (or
-- instance) already claimed this order, or a payment landed meanwhile —
-- the caller must then NOT enqueue the timeout event.
--
-- The status flips to 'payment_timeout' in the same statement: leaving it
-- 'created' would let a late PaymentProcessed still transition the order to
-- 'paid' (via MarkOrderPaymentOutcome's status='created' guard) while the
-- gateway projection — where payment_timeout is terminal — keeps showing
-- payment_timeout forever. With the flip, the late outcome is a no-op and
-- both stores agree.
update orders
set payment_timeout_emitted = true,
    status = 'payment_timeout'
where id = $1 and payment_timeout_emitted = false and status = 'created';
