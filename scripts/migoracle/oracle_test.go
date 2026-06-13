package migoracle_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"
	"go-boilerplate/scripts/migoracle"

	"github.com/stretchr/testify/require"
)

// resolveDir turns a possibly repo-root-relative migration dir into an
// absolute path. `go test` runs with cwd set to the package source directory
// (scripts/migoracle), not the directory the user invoked it from, so a
// relative OLD_MIG/NEW_MIG like examples/<svc>/internal/migrations/sql is
// resolved against the module root (located by walking up to go.mod).
func resolveDir(t *testing.T, dir string) string {
	t.Helper()
	if filepath.IsAbs(dir) {
		return dir
	}
	wd, err := os.Getwd()
	require.NoError(t, err)
	root := wd
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		require.NotEqual(t, parent, root, "go.mod not found above %s", wd)
		root = parent
	}
	return filepath.Join(root, dir)
}

// fingerprintDir spins up a fresh testcontainer DB, applies every goose
// migration in dir, and returns the catalog fingerprint of the result.
//
// dir is a directory on disk containing the *.sql migration files directly
// (e.g. examples/<svc>/internal/migrations/sql).
//
// goose rejects a "." dir argument under os.DirFS(dir) ("'.' directory does
// not exist"), so we root the FS one level up at filepath.Dir(dir) and pass
// filepath.Base(dir) as the goose directory — the leaf name then resolves
// against the FS root.
func fingerprintDir(ctx context.Context, t *testing.T, dir string) string {
	t.Helper()

	dir = resolveDir(t, dir)
	dsn := pgtest.NewDSN(t)

	err := pg.Migrate(ctx, dsn, os.DirFS(filepath.Dir(dir)), filepath.Base(dir))
	require.NoError(t, err, "migrate %s", dir)

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	fp, err := migoracle.Fingerprint(ctx, pool)
	require.NoError(t, err, "fingerprint %s", dir)
	return fp
}

// TestCollapseEquivalent applies the OLD_MIG and NEW_MIG migration directories
// to two fresh databases and asserts their catalog fingerprints are identical.
// It is the oracle that proves a collapsed migration baseline is schema-byte-
// identical to the old chain. Skipped unless both env vars are set.
func TestCollapseEquivalent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	oldDir := os.Getenv("OLD_MIG")
	newDir := os.Getenv("NEW_MIG")
	if oldDir == "" || newDir == "" {
		t.Skip("set OLD_MIG and NEW_MIG to migration sql directories")
	}
	ctx := context.Background()

	fpOld := fingerprintDir(ctx, t, oldDir)
	fpNew := fingerprintDir(ctx, t, newDir)

	require.Equal(t, fpOld, fpNew, "collapsed schema must match old chain")
}
