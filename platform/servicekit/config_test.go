package servicekit_test

import (
	"context"
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

// TestConfig_RetentionInvariantViolated covers the inbox≥topic-retention
// invariant predicate: violated only when TopicRetention>0 AND InboxRetention
// is strictly shorter. TopicRetention=0 (broker default) is never asserted.
func TestConfig_RetentionInvariantViolated(t *testing.T) {
	cases := []struct {
		name        string
		inbox       time.Duration
		topic       time.Duration
		wantViolate bool
	}{
		{"inbox >= topic ok", 168 * time.Hour, 168 * time.Hour, false},
		{"inbox > topic ok", 200 * time.Hour, 168 * time.Hour, false},
		{"inbox < topic violated", 24 * time.Hour, 168 * time.Hour, true},
		{"topic zero not asserted", 1 * time.Hour, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := servicekit.Config{InboxRetention: tc.inbox, TopicRetention: tc.topic}
			assert.Equal(t, tc.wantViolate, cfg.RetentionInvariantViolated())
		})
	}
}

// TestNew_ProductionRejectsRetentionInversion: in production, servicekit.New
// FAILS FAST when INBOX_RETENTION < TOPIC_RETENTION (replay-after-cleanup
// defense), before any telemetry/pg/kafka setup. Outside production the same
// config only warns and proceeds (so this test asserts the production error
// path without needing containers).
func TestNew_ProductionRejectsRetentionInversion(t *testing.T) {
	t.Setenv("APP_ENV", "production")

	cfg := servicekit.Config{
		InboxRetention: 24 * time.Hour,  // shorter than topic — the hole
		TopicRetention: 168 * time.Hour, // 7d redelivery horizon
	}
	cfg.Telemetry.Enabled = false

	_, err := servicekit.New(context.Background(), cfg, nil, "")
	require.Error(t, err, "production must refuse to start with inbox < topic retention")
	assert.Contains(t, err.Error(), "INBOX_RETENTION")
	assert.Contains(t, err.Error(), "replay-after-cleanup")
}
