-- +goose Up
create table audit_log (
    id          bigserial primary key,
    actor       text        not null,
    action      text        not null,
    subject     text        not null,
    metadata    jsonb       not null default '{}'::jsonb,
    created_at  timestamptz not null default now()
);
create index audit_log_action_idx on audit_log (action, created_at);

-- +goose Down
drop table audit_log;
