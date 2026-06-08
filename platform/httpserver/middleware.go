// Package httpserver builds a configured chi HTTP server with a standard
// middleware stack and graceful shutdown.
package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"go-boilerplate/platform/httpx"
	"go-boilerplate/platform/log"
)

type ctxKey int

const requestIDKey ctxKey = iota

const requestIDHeader = "X-Request-Id"

// RequestID ensures every request has an ID, echoed in the response header
// and stored in the context.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = newID()
		}
		w.Header().Set(requestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDFromContext returns the request ID, or "" if absent.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// Recover converts a panic in a downstream handler into a 500 problem+json.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.From(r.Context()).Error("panic recovered",
					"panic", rec, "path", r.URL.Path)
				httpx.Error(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
