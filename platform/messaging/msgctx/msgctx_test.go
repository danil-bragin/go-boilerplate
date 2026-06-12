package msgctx_test

import (
	"context"
	"testing"

	"go-boilerplate/platform/messaging/msgctx"

	"github.com/stretchr/testify/assert"
)

func TestCorrelationID_RoundTrip(t *testing.T) {
	t.Parallel()

	assert.Empty(t, msgctx.CorrelationID(context.Background()), "absent → empty")

	ctx := msgctx.WithCorrelationID(context.Background(), "corr-1")
	assert.Equal(t, "corr-1", msgctx.CorrelationID(ctx))
}

func TestParentMessageID_RoundTrip(t *testing.T) {
	t.Parallel()

	assert.Empty(t, msgctx.ParentMessageID(context.Background()), "absent → empty")

	ctx := msgctx.WithParentMessageID(context.Background(), "msg-7")
	assert.Equal(t, "msg-7", msgctx.ParentMessageID(ctx))
}

// The two ids occupy distinct context keys: setting one must never shadow or
// overwrite the other.
func TestCorrelationAndParentAreIndependent(t *testing.T) {
	t.Parallel()

	ctx := msgctx.WithCorrelationID(context.Background(), "corr-1")
	ctx = msgctx.WithParentMessageID(ctx, "msg-7")

	assert.Equal(t, "corr-1", msgctx.CorrelationID(ctx))
	assert.Equal(t, "msg-7", msgctx.ParentMessageID(ctx))
}

// Later writes win, and overwriting one id leaves the other intact (the
// per-hop causation update pattern used by consume.Typed).
func TestOverwriteSemantics(t *testing.T) {
	t.Parallel()

	ctx := msgctx.WithCorrelationID(context.Background(), "corr-1")
	ctx = msgctx.WithParentMessageID(ctx, "parent-a")
	ctx = msgctx.WithParentMessageID(ctx, "parent-b") // next hop

	assert.Equal(t, "parent-b", msgctx.ParentMessageID(ctx))
	assert.Equal(t, "corr-1", msgctx.CorrelationID(ctx), "correlation is chain-constant")
}

func TestHeaderConstants(t *testing.T) {
	t.Parallel()

	// Wire-frozen: these header names are part of the cross-service contract.
	assert.Equal(t, "correlation-id", msgctx.HeaderCorrelationID)
	assert.Equal(t, "causation-id", msgctx.HeaderCausationID)
}
