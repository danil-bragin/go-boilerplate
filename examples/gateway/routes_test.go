package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/security/authz"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResponseErrorHandler_DoesNotLeakInternalError asserts that an internal
// handler error is mapped to a generic coded 500 — the real error string
// must never reach the client, only the structured log.
func TestResponseErrorHandler_DoesNotLeakInternalError(t *testing.T) {
	const secret = "pg: connect to db host=10.0.0.5 password=hunter2 failed"

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/abc", http.NoBody)
	req = req.WithContext(log.Into(context.Background(), logger))
	rec := httptest.NewRecorder()

	responseErrorHandler(rec, req, errors.New(secret))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	body := rec.Body.String()
	assert.NotContains(t, body, secret, "internal error string must not be echoed to the client")
	assert.NotContains(t, body, "hunter2")
	assert.Contains(t, body, `"code":"INTERNAL"`, "unknown errors map to the INTERNAL code")

	// The real error must be logged for operators.
	assert.Contains(t, logBuf.String(), "hunter2", "real error must be logged")
}

// TestResponseErrorHandler_CodedErrorsKeepStatusAndCode asserts that apperr
// errors returned by handlers reach the client with their registered
// status and code — including the platform auth sentinels that replaced the
// old authError type, and codes wrapped deeper in the chain.
func TestResponseErrorHandler_CodedErrorsKeepStatusAndCode(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"unauthenticated", authz.ErrUnauthenticated, http.StatusUnauthorized, apperr.CodeAuthUnauthenticated},
		{"forbidden (wrapped)", fmt.Errorf("rbac: %w", authz.ErrForbidden), http.StatusForbidden, apperr.CodeAuthForbidden},
		{"order not found", app.OrderNotFound("o-1"), http.StatusNotFound, apperrs.CodeOrderNotFound},
		{"invalid cursor (wrapped)", fmt.Errorf("gateway: list orders: %w", app.ErrInvalidCursor), http.StatusBadRequest, apperrs.CodeInvalidCursor},
		{"idempotency mismatch", apperr.New(apperrs.CodeIdempotencyBodyMismatch), http.StatusConflict, apperrs.CodeIdempotencyBodyMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", http.NoBody)
			rec := httptest.NewRecorder()

			responseErrorHandler(rec, req, tc.err)

			assert.Equal(t, tc.wantStatus, rec.Code)
			assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
			var p struct {
				Status   int    `json:"status"`
				Code     string `json:"code"`
				Instance string `json:"instance"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
			assert.Equal(t, tc.wantStatus, p.Status)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, "/v1/orders/o-1", p.Instance, "instance is the request path")
		})
	}
}

// TestResponseErrorHandler_NotFoundCarriesOrderIDParam pins the AIP-193 rule
// end to end: the order id referenced by the 404 detail is also a param.
func TestResponseErrorHandler_NotFoundCarriesOrderIDParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-42", http.NoBody)
	rec := httptest.NewRecorder()

	responseErrorHandler(rec, req, app.OrderNotFound("o-42"))

	var p struct {
		Detail string         `json:"detail"`
		Params map[string]any `json:"params"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	assert.Equal(t, "order o-42 not found", p.Detail)
	assert.Equal(t, "o-42", p.Params["order_id"])
}

// TestRequestErrorHandler_WritesProblemValidationMessage asserts that request
// binding errors yield a coded 400 (GATEWAY_MALFORMED_REQUEST) whose detail
// echoes the (safe, client-caused) binding message, with the same message
// carried structurally in params.reason.
func TestRequestErrorHandler_WritesProblemValidationMessage(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", strings.NewReader("{"))
	rec := httptest.NewRecorder()

	requestErrorHandler(rec, req, errors.New("unexpected EOF decoding body"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var p struct {
		Code   string         `json:"code"`
		Detail string         `json:"detail"`
		Params map[string]any `json:"params"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	assert.Equal(t, apperrs.CodeMalformedRequest, p.Code)
	assert.Contains(t, p.Detail, "unexpected EOF decoding body")
	assert.Equal(t, "unexpected EOF decoding body", p.Params["reason"])
}
