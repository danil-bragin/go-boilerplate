package testingexamples

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/messaging/outbox"

	"github.com/google/uuid"
)

// ItemResponse is the shape returned by the external catalogue API.
type ItemResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// FetchAndNotify fetches an item from an external HTTP API (baseURL/items/:id),
// caches the raw JSON under "item:<id>", and publishes an outbox event.
//
// This is the function-under-test for functional_example_test.go. It is
// intentionally small — just enough to wire three collaborators so the test
// can demonstrate the functional / component-test pattern.
func FetchAndNotify(
	ctx context.Context,
	baseURL string,
	id string,
	cache cqrs.Cache,
	pub outbox.Publisher,
) error {
	// Call the external HTTP API.
	resp, err := http.Get(baseURL + "/items/" + id) //nolint:noctx
	if err != nil {
		return fmt.Errorf("FetchAndNotify: GET items: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("FetchAndNotify: unexpected status %d", resp.StatusCode)
	}

	var item ItemResponse
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return fmt.Errorf("FetchAndNotify: decode response: %w", err)
	}

	// Cache the raw JSON for downstream reads.
	raw, _ := json.Marshal(item)
	cache.Set(ctx, "item:"+id, raw, time.Minute)

	// Publish an outbox event so other services can react. Constructed
	// directly: production-shaped code must not use platform/testkit
	// builders (fixtures are for _test.go files only — see the arch guard
	// TestTestkitOnlyImportedFromTests).
	msg := outbox.Message{
		ID:            uuid.New(),
		AggregateType: "catalogue.events",
		AggregateID:   item.ID,
		EventType:     "ItemFetched",
		Payload:       raw,
		CreatedAt:     time.Now().UTC(),
	}
	if err := pub.Publish(ctx, msg); err != nil {
		return fmt.Errorf("FetchAndNotify: publish: %w", err)
	}
	return nil
}
