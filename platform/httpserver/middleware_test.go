package httpserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpserver"
)

func TestRequestID_AddsHeaderAndContext(t *testing.T) {
	var seen string
	h := httpserver.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFromContext(r.Context())
		w.WriteHeader(200)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	require.NotEmpty(t, seen)
	require.Equal(t, seen, rec.Header().Get("X-Request-Id"))
}

func TestRecover_TurnsPanicInto500Problem(t *testing.T) {
	h := httpserver.Recover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	require.Equal(t, 500, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
}
