package app_test

import (
	"context"
	"log/slog"
	"testing"

	"go-boilerplate/examples/orders/internal/app"
	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/platform/apperr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOrderHandler_RejectsInvalidOrderID pins the input-validation edge
// of the command path: a non-UUID order id is rejected BEFORE any row is
// written or event enqueued (nil repo/publisher prove nothing is touched).
// The caller supplies the id (idempotent POST), so this is a real attack
// surface. The rule itself lives in order.Service; this test pins that the
// adapter surfaces it unchanged as the permanent ORDERS_INVALID_ORDER_ID.
func TestCreateOrderHandler_RejectsInvalidOrderID(t *testing.T) {
	t.Parallel()
	svc := order.NewService(nil, nil, slog.New(slog.DiscardHandler), 0)
	handler := app.CreateOrderHandler(svc)

	for _, bad := range []string{"", "not-a-uuid", "12345", "ffffffff-ffff-ffff-ffff-fffffffffffg"} {
		_, err := handler(context.Background(), app.CreateOrder{
			OrderID:     bad,
			CustomerID:  "cust-1",
			AmountCents: 100,
			Currency:    "USD",
		})
		require.Error(t, err, "order id %q must be rejected", bad)
		assert.ErrorContains(t, err, "invalid order id")
		assert.Equal(t, order.CodeInvalidOrderID, apperr.Code(err))
		assert.True(t, apperr.IsPermanent(err), "malformed payloads must short-circuit to the DLT")
	}
}
