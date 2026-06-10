package api

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/web/httpx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// fieldRules extracts the {field → rule} pairs from a VALIDATION_FAILED
// apperr's params.fields entries.
func fieldRules(t *testing.T, err error) map[string]string {
	t.Helper()
	var ae *apperr.Error
	require.ErrorAs(t, err, &ae)
	fields, ok := ae.Params["fields"].([]map[string]any)
	require.True(t, ok, "params.fields must be the structured field list, got %T", ae.Params["fields"])
	out := make(map[string]string, len(fields))
	for _, f := range fields {
		out[f["field"].(string)] = f["rule"].(string)
	}
	return out
}

// TestCreateOrder_EdgeValidation400 is the table of invalid bodies: each one
// must be rejected with a permanent VALIDATION_FAILED apperr (→ 400) BEFORE
// any DB read or Kafka produce — proven by the nil pool/producer on the test
// server: an un-rejected request would nil-panic.
func TestCreateOrder_EdgeValidation400(t *testing.T) {
	cases := []struct {
		name      string
		body      CreateOrderRequest
		wantField string
		wantRule  string
	}{
		{
			name:      "negative amount",
			body:      CreateOrderRequest{CustomerId: "c1", AmountCents: -5, Currency: "USD"},
			wantField: "amount_cents",
			wantRule:  "gt",
		},
		{
			name:      "zero amount",
			body:      CreateOrderRequest{CustomerId: "c1", AmountCents: 0, Currency: "USD"},
			wantField: "amount_cents",
			wantRule:  "gt",
		},
		{
			name:      "missing customer id",
			body:      CreateOrderRequest{CustomerId: "", AmountCents: 100, Currency: "USD"},
			wantField: "customer_id",
			wantRule:  "required",
		},
		{
			name:      "customer id too long",
			body:      CreateOrderRequest{CustomerId: strings.Repeat("x", 129), AmountCents: 100, Currency: "USD"},
			wantField: "customer_id",
			wantRule:  "max",
		},
		{
			name:      "currency wrong length",
			body:      CreateOrderRequest{CustomerId: "c1", AmountCents: 100, Currency: "US"},
			wantField: "currency",
			wantRule:  "len",
		},
		{
			name:      "currency lowercase",
			body:      CreateOrderRequest{CustomerId: "c1", AmountCents: 100, Currency: "usd"},
			wantField: "currency",
			wantRule:  "uppercase",
		},
		{
			name:      "currency not in ISO-4217 allowlist",
			body:      CreateOrderRequest{CustomerId: "c1", AmountCents: 100, Currency: "ZZZ"},
			wantField: "currency",
			wantRule:  "iso4217",
		},
	}

	s := newTestServer(nil, nil, true) // nil pool/producer: must fail before I/O
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.body
			_, err := s.CreateOrder(context.Background(), CreateOrderRequestObject{Body: &body})

			require.Error(t, err)
			assert.Equal(t, apperr.CodeValidationFailed, apperr.Code(err))
			assert.True(t, apperr.IsPermanent(err), "validation failures are permanent")

			rules := fieldRules(t, err)
			assert.Equal(t, tc.wantRule, rules[tc.wantField],
				"expected field %q to fail rule %q (got %v)", tc.wantField, tc.wantRule, rules)

			// The edge mapping clients see: 400 + code + structured fields.
			p := httpx.FromError(err)
			assert.Equal(t, http.StatusBadRequest, p.Status)
			assert.Equal(t, apperr.CodeValidationFailed, p.Code)
			assert.NotNil(t, p.Params["fields"])
		})
	}
}

// TestCreateOrder_EdgeValidation_IdempotencyKeyStillValidated: validation
// runs before the idempotency-mismatch DB read — with a key set and a nil
// pool an invalid body must still return 400, not panic on the lookup.
func TestCreateOrder_EdgeValidation_IdempotencyKeyStillValidated(t *testing.T) {
	s := newTestServer(nil, nil, true)
	key := "retry-key"
	_, err := s.CreateOrder(context.Background(), CreateOrderRequestObject{
		Body:   &CreateOrderRequest{CustomerId: "c1", AmountCents: -1, Currency: "USD"},
		Params: CreateOrderParams{IdempotencyKey: &key},
	})
	require.Error(t, err)
	assert.Equal(t, apperr.CodeValidationFailed, apperr.Code(err))
}

// newTestCommand builds a wire command with a fixed valid order id.
func newTestCommand(customerID string, amountCents int64, currency string) *ordersv1.CreateOrderCommand {
	return &ordersv1.CreateOrderCommand{
		OrderId:     "7d8e6a52-1f3b-4e09-9c2a-6d8f0b3e5a71",
		CustomerId:  customerID,
		AmountCents: amountCents,
		Currency:    currency,
	}
}

// TestValidateCreateOrderCommand_ValidBodiesPass: representative allowlist
// currencies and boundary values pass.
func TestValidateCreateOrderCommand_ValidBodiesPass(t *testing.T) {
	for _, currency := range []string{"USD", "EUR", "UAH", "PLN", "SGD"} {
		cmd := newTestCommand("c1", 1, currency)
		assert.NoError(t, validateCreateOrderCommand(cmd), "currency %s", currency)
	}
	// customer_id at the 128 boundary.
	cmd := newTestCommand(strings.Repeat("x", 128), 100, "USD")
	assert.NoError(t, validateCreateOrderCommand(cmd))
}
