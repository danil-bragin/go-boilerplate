-- name: InsertOutbox :exec
insert into outbox (id, topic, aggregate_type, aggregate_id, event_type, payload, headers)
values ($1, $2, $3, $4, $5, $6, $7);

-- name: FetchUnpublished :many
select id, topic, aggregate_type, aggregate_id, event_type, payload, headers, created_at
from outbox
where published_at is null
order by created_at
limit $1
for update skip locked;

-- name: FetchUnpublishedShard :many
-- Sharded fetch: only rows whose aggregate_id hashes into this shard. Uses
-- hashtextextended (the hash family Postgres hash-partitioning also uses) so a
-- given aggregate_id maps to exactly one shard — preserving per-aggregate order
-- while N shard-leaders publish concurrently. The double mod normalizes the
-- signed hash into [0, shard_count).
select id, topic, aggregate_type, aggregate_id, event_type, payload, headers, created_at
from outbox
where published_at is null
  and mod(mod(hashtextextended(aggregate_id, 0), @shard_count::bigint) + @shard_count::bigint, @shard_count::bigint) = @shard_index::bigint
order by created_at
limit @batch_size
for update skip locked;

-- name: MarkPublished :exec
update outbox set published_at = now() where id = $1;

-- name: MarkPublishedBatch :exec
update outbox set published_at = now() where id = any($1::uuid[]);

-- name: DeletePublishedBefore :execrows
delete from outbox where published_at is not null and published_at < $1;
