-- +goose Up
create table orders_read (
    order_id     uuid        primary key,
    customer_id  text        not null,
    amount_cents bigint      not null,
    currency     text        not null,
    status       text        not null,
    updated_at   timestamptz not null default now()
);

create table inbox (
    consumer     text        not null,
    message_id   text        not null,
    processed_at timestamptz not null default now(),
    primary key (consumer, message_id)
);

-- +goose Down
drop table inbox;
drop table orders_read;
