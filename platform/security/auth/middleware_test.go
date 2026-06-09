package auth_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/security/auth"

	"github.com/stretchr/testify/assert"
)

// errVerifier is a Verifier stub that always fails with the given error.
type errVerifier struct{ err error }

func (v errVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return auth.Principal{}, v.err
}

// TestMiddleware_NonTokenError_GenericDetailNotEchoed asserts that verifier
// failures NOT wrapping ErrInvalidToken (e.g. JWKS fetch errors carrying
// internal URLs) produce a generic 401 problem — the internal error string is
// logged, never echoed to the client.
func TestMiddleware_NonTokenError_GenericDetailNotEchoed(t *testing.T) {
	const internal = "jwks fetch http://10.1.2.3:8443/internal-realm failed: connection refused"

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	handler := auth.Middleware(errVerifier{err: errors.New(internal)})(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("next handler must not be called")
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req = req.WithContext(log.Into(req.Context(), logger))
	req.Header.Set("Authorization", "Bearer some.token.value")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	assert.NotContains(t, rec.Body.String(), "10.1.2.3",
		"internal error detail must not be echoed to the client")
	assert.NotContains(t, rec.Body.String(), "connection refused")

	assert.Contains(t, logBuf.String(), "connection refused", "real error must be logged")
}

// TestMiddleware_InvalidTokenError_StaysGeneric keeps the existing contract:
// ErrInvalidToken-wrapped failures respond with the generic "invalid token".
func TestMiddleware_InvalidTokenError_StaysGeneric(t *testing.T) {
	wrapped := errors.Join(auth.ErrInvalidToken, errors.New("exp not satisfied"))
	handler := auth.Middleware(errVerifier{err: wrapped})(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("next handler must not be called")
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer some.token.value")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid token")
}
