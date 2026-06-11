-- name: QueryByActor :many
-- DSAR/audit read path: one actor's audit trail, newest first. since is
-- INCLUSIVE (pass the zero time for "everything"); row_limit caps the result
-- at the newest rows. Backed by the (actor, created_at) index.
select id, actor, action, subject, metadata, created_at
from audit_log
where actor = sqlc.arg(actor)::text
  and created_at >= sqlc.arg(since)::timestamptz
order by created_at desc, id desc
limit sqlc.arg(row_limit)::int;
