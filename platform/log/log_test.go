package log_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/log"
)

func TestNew_WritesStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(log.Config{Level: "debug", Format: "json"}, &buf)

	logger.Info("hello", "key", "value")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "hello", entry["msg"])
	require.Equal(t, "value", entry["key"])
}

func TestParseLevel(t *testing.T) {
	require.Equal(t, "WARN", log.ParseLevel("warn").String())
	require.Equal(t, "INFO", log.ParseLevel("nonsense").String()) // fallback
}

func TestContextLogger_RoundTrips(t *testing.T) {
	var buf bytes.Buffer
	base := log.New(log.Config{Level: "info", Format: "json"}, &buf)

	ctx := log.Into(context.Background(), base.With("svc", "orders"))
	log.From(ctx).Info("msg")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "orders", entry["svc"])
}
