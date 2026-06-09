package testingexamples

import (
	"context"
	"fmt"

	"go-boilerplate/platform/messaging/outbox"
)

// PublishEvent is the tiny function-under-test used by unit_example_test.go.
// It is intentionally small — enough to show how to wrap a collaborator and
// test it in isolation without any I/O.
//
// In a real service this would live alongside the domain logic it orchestrates
// (e.g. inside an application-layer command handler).
func PublishEvent(ctx context.Context, pub outbox.Publisher, msg outbox.Message) error {
	if err := pub.Publish(ctx, msg); err != nil {
		return fmt.Errorf("publishEvent: %w", err)
	}
	return nil
}
