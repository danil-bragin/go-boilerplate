package app_test

import (
	"context"
	"testing"

	"go-boilerplate/examples/orders/internal/app"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOrderHandler_RejectsInvalidOrderID pins the input-validation edge
// of the command handler: a non-UUID order id is rejected BEFORE any row is
// written or event enqueued (nil pool/repo prove nothing is touched). The
// caller supplies the id (idempotent POST), so this is a real attack surface.
func TestCreateOrderHandler_RejectsInvalidOrderID(t *testing.T) {
	t.Parallel()
	handler := app.CreateOrderHandler(nil, nil)

	for _, bad := range []string{"", "not-a-uuid", "12345", "ffffffff-ffff-ffff-ffff-fffffffffffg"} {
		_, err := handler(context.Background(), app.CreateOrder{
			OrderID:     bad,
			CustomerID:  "cust-1",
			AmountCents: 100,
			Currency:    "USD",
		})
		require.Error(t, err, "order id %q must be rejected", bad)
		assert.ErrorContains(t, err, "invalid order id")
	}
}
