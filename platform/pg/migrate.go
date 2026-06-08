package pg

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver needed by goose
	"github.com/pressly/goose/v3"
)

// Migrate applies all up-migrations from fsys (rooted at dir) to the database
// at dsn using goose. Migrations are plain *.sql files with goose Up/Down
// annotations. It opens a short-lived database/sql connection via the pgx
// stdlib driver because goose operates on *sql.DB.
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("pg: open sql db for migration: %w", err)
	}
	defer func() { _ = db.Close() }()

	goose.SetBaseFS(fsys)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("pg: goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, dir); err != nil {
		return fmt.Errorf("pg: goose up: %w", err)
	}
	return nil
}
