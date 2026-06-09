package config_test

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"go-boilerplate/platform/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type secretConfig struct {
	Endpoint string        `env:"SEC_ENDPOINT" envDefault:"localhost:9000"`
	APIKey   config.Secret `env:"SEC_API_KEY"`
}

// TestSecret_EnvParsingAndReveal: Secret fields parse from env like plain
// strings and Reveal() returns the raw value.
func TestSecret_EnvParsingAndReveal(t *testing.T) {
	t.Setenv("SEC_API_KEY", "hunter2")

	cfg, err := config.Load[secretConfig]()
	require.NoError(t, err)
	assert.Equal(t, "hunter2", cfg.APIKey.Reveal())
}

// TestSecret_NeverPrintsValue: every print path — %v, %+v, %#v, %s, fmt.Sprint,
// and slog output — must show [REDACTED], never the raw secret.
func TestSecret_NeverPrintsValue(t *testing.T) {
	t.Setenv("SEC_API_KEY", "hunter2")
	cfg, err := config.Load[secretConfig]()
	require.NoError(t, err)

	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		out := fmt.Sprintf(format, cfg)
		assert.NotContains(t, out, "hunter2", "format %s leaked the secret", format)
		assert.Contains(t, out, "[REDACTED]", "format %s must redact", format)
	}

	// slog: both the struct field and the bare Secret value.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logger.Info("config loaded", "cfg", cfg, "key", cfg.APIKey)
	assert.NotContains(t, buf.String(), "hunter2", "slog output leaked the secret")
	assert.Contains(t, buf.String(), "[REDACTED]")
}

// TestSecret_LogValueRedacted: the slog.LogValuer implementation resolves to
// the redacted placeholder.
func TestSecret_LogValueRedacted(t *testing.T) {
	s := config.Secret("topsecret")
	assert.Equal(t, "[REDACTED]", s.LogValue().String())
	assert.Equal(t, "[REDACTED]", s.String())
	assert.Equal(t, "[REDACTED]", s.GoString())
	assert.Equal(t, "topsecret", s.Reveal())
}

// validatedConfig implements the Validate hook.
type validatedConfig struct {
	Port int `env:"VAL_PORT" envDefault:"0"`
}

func (c validatedConfig) Validate() error {
	if c.Port <= 0 {
		return fmt.Errorf("port must be positive, got %d", c.Port)
	}
	return nil
}

// TestLoad_ValidateHookFires: when the config type implements
// `Validate() error`, Load calls it after env parsing and wraps the failure.
func TestLoad_ValidateHookFires(t *testing.T) {
	t.Setenv("VAL_PORT", "0")
	_, err := config.Load[validatedConfig]()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config: validate")
	assert.Contains(t, err.Error(), "port must be positive")

	t.Setenv("VAL_PORT", "8080")
	cfg, err := config.Load[validatedConfig]()
	require.NoError(t, err)
	assert.Equal(t, 8080, cfg.Port)
}

// ptrValidatedConfig implements Validate on the POINTER receiver — Load must
// still find it.
type ptrValidatedConfig struct {
	Name string `env:"VAL_NAME" envDefault:""`
}

func (c *ptrValidatedConfig) Validate() error {
	if c.Name == "" {
		return errors.New("name required")
	}
	return nil
}

func TestLoad_ValidateHookPointerReceiver(t *testing.T) {
	t.Setenv("VAL_NAME", "")
	_, err := config.Load[ptrValidatedConfig]()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name required")

	t.Setenv("VAL_NAME", "svc")
	_, err = config.Load[ptrValidatedConfig]()
	require.NoError(t, err)
}
