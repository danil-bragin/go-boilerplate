package servicekit_test

import (
	"os"
	"testing"

	"go-boilerplate/platform/storage/pg/pgtest"
)

// TestMain terminates the package-shared Postgres container (started lazily
// by pgtest.SharedDSN in the periodic-worker tests) after all tests ran.
// Tests that use per-test containers (pgtest.NewDSN / kafkatest.NewRedpanda)
// are unaffected.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.TerminateShared()
	os.Exit(code)
}
