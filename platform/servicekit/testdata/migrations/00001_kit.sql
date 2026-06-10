-- +goose Up
CREATE TABLE kit_migrate_marker (id int PRIMARY KEY);

-- +goose Down
DROP TABLE kit_migrate_marker;
