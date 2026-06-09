package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"go-boilerplate/platform/config"

	"github.com/stretchr/testify/require"
)

type sampleConfig struct {
	Port     int    `env:"PORT" envDefault:"8080"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
	DSN      string `env:"DSN,required"`
}

func TestLoad_UsesDefaultsAndEnv(t *testing.T) {
	t.Setenv("DSN", "postgres://localhost/db")
	t.Setenv("PORT", "9090")

	cfg, err := config.Load[sampleConfig]()
	require.NoError(t, err)
	require.Equal(t, 9090, cfg.Port)
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, "postgres://localhost/db", cfg.DSN)
}

func TestLoad_MissingRequiredFails(t *testing.T) {
	os.Unsetenv("DSN")
	_, err := config.Load[sampleConfig]()
	require.Error(t, err)
}

func TestLoad_NonStructTypeReturnsClearError(t *testing.T) {
	_, err := config.Load[int]()
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a struct")

	_, err = config.Load[map[string]string]()
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a struct")
}

type fileConfig struct {
	Name string `env:"NAME"`
	Port int    `env:"PORT" envDefault:"3000"`
}

func TestLoadFromFile_ReadsDotEnvFile(t *testing.T) {
	// Ensure env vars are not set so the file values are used.
	os.Unsetenv("NAME")
	os.Unsetenv("PORT")

	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	require.NoError(t, os.WriteFile(path, []byte("NAME=from-file\nPORT=5000\n"), 0o600))

	cfg, err := config.LoadFromFile[fileConfig](path)
	require.NoError(t, err)
	require.Equal(t, "from-file", cfg.Name)
	require.Equal(t, 5000, cfg.Port)
}

func TestLoadFromFile_EnvOverridesFile(t *testing.T) {
	t.Setenv("NAME", "from-env")
	os.Unsetenv("PORT")

	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	require.NoError(t, os.WriteFile(path, []byte("NAME=from-file\nPORT=5000\n"), 0o600))

	cfg, err := config.LoadFromFile[fileConfig](path)
	require.NoError(t, err)
	require.Equal(t, "from-env", cfg.Name)
	// PORT not set in env, so file value applies
	require.Equal(t, 5000, cfg.Port)
}

func TestLoadFromFile_MissingFileFails(t *testing.T) {
	_, err := config.LoadFromFile[fileConfig]("/no/such/file.env")
	require.Error(t, err)
}

func TestLoadFromFile_NonStructFails(t *testing.T) {
	_, err := config.LoadFromFile[int]("whatever")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a struct")
}
