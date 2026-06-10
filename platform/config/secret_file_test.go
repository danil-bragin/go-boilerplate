package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/config"
)

// writeSecretFile writes content to a temp file and returns its path.
func writeSecretFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

type fileSecretConfig struct {
	Token config.Secret `env:"FILETEST_TOKEN"`
	Plain string        `env:"FILETEST_PLAIN"`
}

// TestLoad_SecretFromFile pins the _FILE fallback: when the env var is unset
// and <NAME>_FILE points at a file, the Secret field is populated from the
// file's contents with the trailing newline trimmed (Docker/K8s secret mounts
// usually end in \n).
func TestLoad_SecretFromFile(t *testing.T) {
	path := writeSecretFile(t, "hunter2\n")
	t.Setenv("FILETEST_TOKEN_FILE", path)

	cfg, err := config.Load[fileSecretConfig]()
	require.NoError(t, err)
	assert.Equal(t, "hunter2", cfg.Token.Reveal(), "file contents minus trailing newline")
}

// TestLoad_EnvWinsOverFile pins precedence: an explicitly set env var beats
// the _FILE indirection.
func TestLoad_EnvWinsOverFile(t *testing.T) {
	path := writeSecretFile(t, "from-file")
	t.Setenv("FILETEST_TOKEN", "from-env")
	t.Setenv("FILETEST_TOKEN_FILE", path)

	cfg, err := config.Load[fileSecretConfig]()
	require.NoError(t, err)
	assert.Equal(t, "from-env", cfg.Token.Reveal())
}

// TestLoad_MissingSecretFileFails pins fail-fast: a dangling _FILE reference
// is a deployment bug — silently starting with an empty credential would
// produce confusing auth failures much later.
func TestLoad_MissingSecretFileFails(t *testing.T) {
	t.Setenv("FILETEST_TOKEN_FILE", filepath.Join(t.TempDir(), "does-not-exist"))

	_, err := config.Load[fileSecretConfig]()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FILETEST_TOKEN_FILE")
}

// TestLoad_FileIgnoredForNonSecretFields pins scope: _FILE indirection only
// applies to config.Secret fields — plain strings are left alone.
func TestLoad_FileIgnoredForNonSecretFields(t *testing.T) {
	path := writeSecretFile(t, "should-not-load")
	t.Setenv("FILETEST_PLAIN_FILE", path)

	cfg, err := config.Load[fileSecretConfig]()
	require.NoError(t, err)
	assert.Empty(t, cfg.Plain)
}

type fileSecretInner struct {
	DSN config.Secret `env:"FILETEST_DSN" envDefault:"postgres://default"`
}

// FileSecretEmbedded is exported because embedded fields of unexported types
// are themselves unexported — neither caarlos0/env nor the _FILE pass can set
// through them. Real configs embed exported types (servicekit.Config embeds
// pg.Config → field name "Config").
type FileSecretEmbedded struct {
	Key config.Secret `env:"FILETEST_EMBEDDED_KEY"`
}

type fileSecretOuter struct {
	FileSecretEmbedded

	PG   fileSecretInner
	Name string `env:"FILETEST_NAME"`
}

// TestLoad_NestedAndEmbeddedSecrets pins recursion: Secret fields inside
// nested named structs AND embedded structs both get the _FILE treatment
// (servicekit.Config embeds pg.Config — this is the real shape).
// It also pins that _FILE beats envDefault: the default DSN is a dev
// convenience, an explicit file mount is operator intent.
func TestLoad_NestedAndEmbeddedSecrets(t *testing.T) {
	dsnPath := writeSecretFile(t, "postgres://app:s3cret@db:5432/app\n")
	keyPath := writeSecretFile(t, "embedded-key")
	t.Setenv("FILETEST_DSN_FILE", dsnPath)
	t.Setenv("FILETEST_EMBEDDED_KEY_FILE", keyPath)

	cfg, err := config.Load[fileSecretOuter]()
	require.NoError(t, err)
	assert.Equal(t, "postgres://app:s3cret@db:5432/app", cfg.PG.DSN.Reveal(),
		"_FILE must override envDefault for nested struct fields")
	assert.Equal(t, "embedded-key", cfg.Key.Reveal(),
		"embedded struct fields must be reached by the recursion")
}

// TestLoad_EnvDefaultKeptWithoutFile pins the no-op path: with neither the
// env var nor _FILE set, envDefault survives untouched.
func TestLoad_EnvDefaultKeptWithoutFile(t *testing.T) {
	cfg, err := config.Load[fileSecretOuter]()
	require.NoError(t, err)
	assert.Equal(t, "postgres://default", cfg.PG.DSN.Reveal())
}

// TestLoadFromFile_SecretFromFile pins that the .env loader path applies the
// same _FILE pass.
func TestLoadFromFile_SecretFromFile(t *testing.T) {
	secretPath := writeSecretFile(t, "dotenv-secret\n")
	envFile := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(envFile, []byte("FILETEST_TOKEN_FILE="+secretPath+"\n"), 0o600))
	// godotenv loads into the process env; make sure the key is cleaned up.
	t.Setenv("FILETEST_TOKEN_FILE", "")
	require.NoError(t, os.Unsetenv("FILETEST_TOKEN_FILE"))

	cfg, err := config.LoadFromFile[fileSecretConfig](envFile)
	require.NoError(t, err)
	assert.Equal(t, "dotenv-secret", cfg.Token.Reveal())
}
