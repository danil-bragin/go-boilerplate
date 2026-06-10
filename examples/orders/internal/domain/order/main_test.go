package order_test

import (
	"os"
	"testing"

	"go-boilerplate/platform/storage/pg/pgtest"
)

// TestMain tears down the package-shared Postgres container used by the
// PgRepository integration tests (pgtest.SharedDSN contract).
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.TerminateShared()
	os.Exit(code)
}
