package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/config"
)

type sampleConfig struct {
	Port     int    `env:"PORT" env-default:"8080"`
	LogLevel string `env:"LOG_LEVEL" env-default:"info"`
	DSN      string `env:"DSN" env-required:"true"`
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
	Name string `yaml:"name" env:"NAME"`
	Port int    `yaml:"port" env:"PORT" env-default:"3000"`
}

func TestLoadFromFile_ReadsYAMLThenOverlaysEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("name: from-file\nport: 5000\n"), 0o600))

	cfg, err := config.LoadFromFile[fileConfig](path)
	require.NoError(t, err)
	require.Equal(t, "from-file", cfg.Name)
	require.Equal(t, 5000, cfg.Port)
}

func TestLoadFromFile_MissingFileFails(t *testing.T) {
	_, err := config.LoadFromFile[fileConfig]("/no/such/file.yaml")
	require.Error(t, err)
}

func TestLoadFromFile_NonStructFails(t *testing.T) {
	_, err := config.LoadFromFile[int]("whatever")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a struct")
}
