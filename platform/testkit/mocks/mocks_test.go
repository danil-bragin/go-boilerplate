package mocks_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/testkit/mocks"
)

func TestPublisherMock(t *testing.T) {
	mock := &mocks.PublisherMock{
		PublishFunc: func(_ context.Context, _ outbox.Message) error {
			return nil
		},
	}

	ctx := context.Background()
	msg := outbox.Message{
		ID:            uuid.New(),
		AggregateType: "orders.events",
		AggregateID:   "agg-1",
		EventType:     "Created",
		Payload:       []byte("{}"),
	}

	err := mock.Publish(ctx, msg)
	require.NoError(t, err)

	calls := mock.PublishCalls()
	assert.Len(t, calls, 1)
	assert.Equal(t, msg, calls[0].Msg)
}
