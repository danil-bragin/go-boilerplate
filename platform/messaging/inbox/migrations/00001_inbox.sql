-- +goose Up
create table inbox (
    consumer     text        not null,
    message_id   text        not null,
    processed_at timestamptz not null default now(),
    primary key (consumer, message_id)
);

-- +goose Down
drop table inbox;
