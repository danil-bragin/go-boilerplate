-- name: InsertOutbox :exec
insert into outbox (id, aggregate_type, aggregate_id, event_type, payload, headers)
values ($1, $2, $3, $4, $5, $6);

-- name: FetchUnpublished :many
select id, aggregate_type, aggregate_id, event_type, payload, headers, created_at
from outbox
where published_at is null
order by created_at
limit $1
for update skip locked;

-- name: MarkPublished :exec
update outbox set published_at = now() where id = $1;
