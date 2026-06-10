package order_test

import (
	"testing"

	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/platform/apperr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransition_Table pins the order status state machine declaratively:
// 'created' is the only state with outgoing edges (the three terminal payment
// outcomes); every terminal state is a dead end; unknown statuses go nowhere.
// Pure — no Docker, runs in -short.
func TestTransition_Table(t *testing.T) {
	t.Parallel()

	allowed := []struct{ from, to order.Status }{
		{order.StatusCreated, order.StatusPaid},
		{order.StatusCreated, order.StatusPaymentFailed},
		{order.StatusCreated, order.StatusPaymentTimeout},
	}
	for _, tt := range allowed {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			t.Parallel()
			assert.True(t, order.CanTransition(tt.from, tt.to))
			assert.NoError(t, order.Transition(tt.from, tt.to))
		})
	}

	statuses := []order.Status{
		order.StatusCreated, order.StatusPaid,
		order.StatusPaymentFailed, order.StatusPaymentTimeout,
	}
	isAllowed := func(from, to order.Status) bool {
		for _, a := range allowed {
			if a.from == from && a.to == to {
				return true
			}
		}
		return false
	}
	for _, from := range statuses {
		for _, to := range statuses {
			if isAllowed(from, to) {
				continue
			}
			t.Run("forbidden "+string(from)+"->"+string(to), func(t *testing.T) {
				t.Parallel()
				assert.False(t, order.CanTransition(from, to))

				err := order.Transition(from, to)
				require.Error(t, err)

				var ae *apperr.Error
				require.ErrorAs(t, err, &ae)
				assert.Equal(t, order.CodeInvalidStatusTransition, ae.Code)
				assert.Equal(t, 409, ae.Status)
				assert.True(t, ae.Permanent, "an invalid transition can never succeed on retry: it must go straight to the DLT")
				assert.Equal(t, string(from), ae.Params["from"])
				assert.Equal(t, string(to), ae.Params["to"])
			})
		}
	}

	// Unknown statuses (bad data, future migrations) never transition.
	assert.False(t, order.CanTransition("pending", order.StatusPaid))
	assert.False(t, order.CanTransition(order.StatusCreated, "shipped"))
}

// TestCodes_RegisteredWithTemplateInvariant extends the apperr registered-codes
// doc test to the ORDERS_* block: each code is registered with the expected
// edge mapping and every {placeholder} in its message template is declared in
// the registration's Params (Google AIP-193 rule — same invariant
// platform/apperr enforces for its own codes).
func TestCodes_RegisteredWithTemplateInvariant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code      string
		status    int
		permanent bool
	}{
		{order.CodeInvalidStatusTransition, 409, true},
		{order.CodeInvalidOrderID, 400, true},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			t.Parallel()
			reg, ok := apperr.Lookup(tt.code)
			require.True(t, ok, "code %s must be registered from the order package init()", tt.code)
			assert.Equal(t, tt.status, reg.Status)
			assert.Equal(t, tt.permanent, reg.Permanent)

			declared := make(map[string]bool, len(reg.Params))
			for _, p := range reg.Params {
				declared[p] = true
			}
			for _, v := range apperr.TemplateVars(reg.Message) {
				assert.True(t, declared[v],
					"code %s: template variable {%s} not declared in Params %v", tt.code, v, reg.Params)
			}
		})
	}
}
