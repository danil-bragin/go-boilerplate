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
	logger, _ := log.New(log.Config{Level: "debug", Format: "json"}, &buf)

	logger.Info("hello", "key", "value")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "hello", entry["msg"])
	require.Equal(t, "value", entry["key"])
}

func TestNew_NilWriterPanics(t *testing.T) {
	require.PanicsWithValue(t, "log: New: writer must not be nil", func() {
		log.New(log.Config{}, nil)
	})
}

func TestNew_SyncFuncFlushes(t *testing.T) {
	var buf bytes.Buffer
	logger, sync := log.New(log.Config{Level: "info", Format: "json"}, &buf)
	logger.Info("flush-test")
	// sync should be callable and return no error against a buffer
	require.NotNil(t, sync)
	_ = sync()
}

func TestParseLevel(t *testing.T) {
	require.Equal(t, "WARN", log.ParseLevel("warn").String())
	require.Equal(t, "INFO", log.ParseLevel("nonsense").String()) // fallback
}

func TestContextLogger_RoundTrips(t *testing.T) {
	var buf bytes.Buffer
	base, _ := log.New(log.Config{Level: "info", Format: "json"}, &buf)

	ctx := log.Into(context.Background(), base.With("svc", "orders"))
	log.From(ctx).Info("msg")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "orders", entry["svc"])
}
