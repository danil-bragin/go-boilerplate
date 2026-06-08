package pg_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/pg"
)

func TestConfig_BuildPoolConfigAppliesSizingAndDefaults(t *testing.T) {
	cfg := pg.Config{
		DSN:             "postgres://u:p@localhost:5432/db",
		MaxConns:        20,
		MinConns:        2,
		MaxConnLifetime: 0, // exercise default-fill
	}
	pc, err := cfg.BuildPoolConfig()
	require.NoError(t, err)
	require.Equal(t, int32(20), pc.MaxConns)
	require.Equal(t, int32(2), pc.MinConns)
	require.NotZero(t, pc.MaxConnLifetime, "zero lifetime must be defaulted")
}
