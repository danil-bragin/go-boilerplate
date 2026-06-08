-- +goose Up
create table widget (
    id   bigserial primary key,
    name text not null
);

-- +goose Down
drop table widget;
