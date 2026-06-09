package gateway

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-boilerplate/platform/observability/log"

	"github.com/stretchr/testify/assert"
)

// TestResponseErrorHandler_DoesNotLeakInternalError asserts that an internal
// handler error is mapped to a generic RFC7807 500 — the real error string
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
	assert.Contains(t, body, `"internal error"`)

	// The real error must be logged for operators.
	assert.Contains(t, logBuf.String(), "hunter2", "real error must be logged")
}

// TestRequestErrorHandler_WritesProblemValidationMessage asserts that request
// binding errors yield a 400 problem+json with the (safe, client-caused)
// validation message.
func TestRequestErrorHandler_WritesProblemValidationMessage(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", strings.NewReader("{"))
	rec := httptest.NewRecorder()

	requestErrorHandler(rec, req, errors.New("unexpected EOF decoding body"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "unexpected EOF decoding body")
}
