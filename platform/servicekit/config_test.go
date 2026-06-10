package servicekit_test

import (
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/servicekit"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig_ConsumerRetryDefaults: the in-process retry knobs default to the
// values AddConsumer previously hardcoded (3 attempts, 100ms backoff).
func TestConfig_ConsumerRetryDefaults(t *testing.T) {
	cfg, err := config.Load[servicekit.Config]()
	require.NoError(t, err)
	assert.Equal(t, 3, cfg.ConsumerRetryMaxAttempts)
	assert.Equal(t, 100*time.Millisecond, cfg.ConsumerRetryBackoff)
}

// TestConfig_ConsumerRetryFromEnv: the knobs are env-tunable.
func TestConfig_ConsumerRetryFromEnv(t *testing.T) {
	t.Setenv("CONSUMER_RETRY_MAX_ATTEMPTS", "5")
	t.Setenv("CONSUMER_RETRY_BACKOFF", "250ms")
	cfg, err := config.Load[servicekit.Config]()
	require.NoError(t, err)
	assert.Equal(t, 5, cfg.ConsumerRetryMaxAttempts)
	assert.Equal(t, 250*time.Millisecond, cfg.ConsumerRetryBackoff)
}
