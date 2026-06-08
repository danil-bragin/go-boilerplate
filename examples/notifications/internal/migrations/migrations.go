// Package migrations embeds the SQL migration files for the notifications service.
package migrations

import "embed"

// FS contains all goose migration files for the notifications service.
//
//go:embed sql
var FS embed.FS
