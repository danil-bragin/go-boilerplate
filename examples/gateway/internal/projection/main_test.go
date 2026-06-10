package projection_test

import (
	"os"
	"testing"

	"go-boilerplate/platform/storage/pg/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.TerminateShared()
	os.Exit(code)
}
