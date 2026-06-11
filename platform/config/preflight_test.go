package config_test

import (
	"errors"
	"testing"

	"go-boilerplate/platform/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRequireProductionSafety_SkipsOutsideProduction: when APP_ENV is unset or
// not "production", the checks are NOT run — dev/test keep insecure-but-
// convenient defaults (sslmode=disable, CORS="*", etc.).
func TestRequireProductionSafety_SkipsOutsideProduction(t *testing.T) {
	failing := func() error { return errors.New("should not run") }

	for _, env := range []string{"", "development", "dev", "staging", "test"} {
		t.Run("APP_ENV="+env, func(t *testing.T) {
			if env == "" {
				t.Setenv("APP_ENV", "") // explicit empty
			} else {
				t.Setenv("APP_ENV", env)
			}
			require.NoError(t, config.RequireProductionSafety(failing),
				"checks must not run outside production")
		})
	}
}

// TestRequireProductionSafety_RunsInProduction: with APP_ENV=production the
// checks run and any failure is surfaced (aggregated).
func TestRequireProductionSafety_RunsInProduction(t *testing.T) {
	t.Setenv("APP_ENV", "production")

	t.Run("all pass", func(t *testing.T) {
		require.NoError(t, config.RequireProductionSafety(
			func() error { return nil },
			func() error { return nil },
		))
	})

	t.Run("aggregates failures", func(t *testing.T) {
		err := config.RequireProductionSafety(
			func() error { return errors.New("auth disabled") },
			func() error { return nil },
			func() error { return errors.New("cors wildcard") },
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "auth disabled")
		assert.Contains(t, err.Error(), "cors wildcard")
	})
}

// TestIsProduction reflects the APP_ENV reading the guard uses.
func TestIsProduction(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	assert.True(t, config.IsProduction())

	t.Setenv("APP_ENV", "Production") // case-insensitive
	assert.True(t, config.IsProduction())

	t.Setenv("APP_ENV", "development")
	assert.False(t, config.IsProduction())

	t.Setenv("APP_ENV", "")
	assert.False(t, config.IsProduction())
}
