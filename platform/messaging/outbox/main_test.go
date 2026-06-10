package outbox_test

import (
	"os"
	"testing"

	"go-boilerplate/platform/storage/pg/pgtest"
)

// TestMain terminates the package-shared Postgres container (started lazily
// by the first pgtest.SharedDSN call) after all tests have run. Each test
// still gets its own database inside it, so isolation is unchanged.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.TerminateShared()
	os.Exit(code)
}
