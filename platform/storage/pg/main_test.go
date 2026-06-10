package pg

import (
	"os"
	"testing"

	"go-boilerplate/platform/storage/pg/pgtest"
)

// TestMain terminates the package-shared Postgres container (started lazily
// by pgtest.SharedDSN in the leader-election tests) after all tests ran.
// Tests that use pgtest.NewDSN are unaffected (per-test containers).
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.TerminateShared()
	os.Exit(code)
}
