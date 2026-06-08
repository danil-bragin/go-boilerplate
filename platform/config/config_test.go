package config_test

import (
	"os"
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
