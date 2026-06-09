package gateway

import (
	"io/fs"

	"go-boilerplate/examples/gateway/internal/migrations"
)

// Migrations exposes this service's embedded goose migrations (rooted at
// "sql") for ops tooling outside the internal tree — cmd/migrate runs them
// as a standalone pre-deploy job (just migrate gateway).
var Migrations fs.FS = migrations.FS
