package apperrs_test

import (
	"net/http"
	"testing"

	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRegistry_GatewayCodes pins the edge mapping of every GATEWAY_* code:
// status, permanence, and that errors built from them carry the code through
// httpx.FromError-compatible accessors (apperr.Code).
func TestRegistry_GatewayCodes(t *testing.T) {
	cases := []struct {
		code   string
		status int
	}{
		{apperrs.CodeOrderNotFound, http.StatusNotFound},
		{apperrs.CodeInvalidCursor, http.StatusBadRequest},
		{apperrs.CodeIdempotencyBodyMismatch, http.StatusConflict},
		{apperrs.CodeMalformedRequest, http.StatusBadRequest},
		{apperrs.CodeInvalidTimezone, http.StatusBadRequest},
		{apperrs.CodeAttachmentsDisabled, http.StatusNotFound},
		{apperrs.CodeAttachmentInvalidOrderID, http.StatusBadRequest},
		{apperrs.CodeAttachmentInvalidFilename, http.StatusBadRequest},
		{apperrs.CodeAttachmentNotFound, http.StatusNotFound},
		{apperrs.CodeAttachmentTypeRejected, http.StatusUnsupportedMediaType},
		{apperrs.CodeAttachmentTooLarge, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			reg, ok := apperr.Lookup(tc.code)
			require.True(t, ok, "code %s must be registered at init", tc.code)
			assert.Equal(t, tc.status, reg.Status)
			assert.True(t, reg.Permanent, "edge errors describe client input — always permanent")

			err := apperr.New(tc.code)
			assert.Equal(t, tc.code, apperr.Code(err))
			assert.Equal(t, tc.status, err.Status)
		})
	}
}

// TestRegistry_MessageParamsRendered asserts the AIP-193 rule holds for the
// parameterized gateway messages: every template variable is fed by params
// and renders into the developer message.
func TestRegistry_MessageParamsRendered(t *testing.T) {
	err := apperr.New(apperrs.CodeOrderNotFound).WithParam("order_id", "abc-123")
	assert.Equal(t, "order abc-123 not found", err.Message())

	err = apperr.New(apperrs.CodeMalformedRequest).WithParam("reason", "unexpected EOF")
	assert.Equal(t, "malformed request: unexpected EOF", err.Message())
}
