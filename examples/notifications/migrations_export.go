package notifications

import (
	"io/fs"

	"go-boilerplate/examples/notifications/internal/migrations"
)

// Migrations exposes this service's embedded goose migrations (rooted at
// "sql") for ops tooling outside the internal tree — cmd/migrate runs them
// as a standalone pre-deploy job (just migrate notifications).
var Migrations fs.FS = migrations.FS
