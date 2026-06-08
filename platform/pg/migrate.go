package pg

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver needed by goose
	"github.com/pressly/goose/v3"
)

// migrateMu serializes concurrent Migrate calls because goose uses
// process-global state (SetBaseFS, SetDialect).
var migrateMu sync.Mutex

// Migrate applies all up-migrations from fsys (rooted at dir) to the database
// at dsn using goose. Migrations are plain *.sql files with goose Up/Down
// annotations. It opens a short-lived database/sql connection via the pgx
// stdlib driver because goose operates on *sql.DB.
//
// Migrate serializes concurrent calls internally (goose uses process-global
// state).
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string) error {
	migrateMu.Lock()
	defer migrateMu.Unlock()

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
