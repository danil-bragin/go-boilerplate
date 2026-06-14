package ledger

import "embed"

// Migrations holds the canonical Postgres schema for the Postgres-backed Store
// (PgStore). Apply it with pg.Migrate(ctx, dsn, ledger.Migrations, "migrations"),
// or copy 0001_ledger.sql into your service's own migration chain.
//
//go:embed migrations/*.sql
var Migrations embed.FS
