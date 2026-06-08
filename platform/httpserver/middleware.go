// Package httpserver builds a configured chi HTTP server with a standard
// middleware stack and graceful shutdown.
package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/http"
	"runtime/debug"
	"time"

	"go-boilerplate/platform/httpx"
	"go-boilerplate/platform/log"
)

type ctxKey int

const requestIDKey ctxKey = iota

const requestIDHeader = "X-Request-Id"

// maxRequestIDLen is the maximum number of bytes accepted in an incoming
// X-Request-Id header before it is replaced with a fresh id.
const maxRequestIDLen = 128

// validRequestID returns true when s is a non-empty string of at most
// maxRequestIDLen printable ASCII characters (no control characters, CR, LF,
// NUL, or DEL).
func validRequestID(s string) bool {
	if s == "" || len(s) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c == 0x7f { // control chars including CR/LF/NUL/DEL
			return false
		}
	}
	return true
}

// RequestID ensures every request has an ID, echoed in the response header
// and stored in the context. If the incoming X-Request-Id header is absent,
// too long, or contains control characters it is replaced with a fresh
// random id.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if !validRequestID(id) {
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
// Two special cases:
//  1. http.ErrAbortHandler is re-panicked so net/http can abort the connection
//     cleanly (stdlib contract).
//  2. If the response has already been (partially) written, Recover logs the
//     panic but does NOT write any additional bytes — appending to a committed
//     response would corrupt it.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cw := &capturingWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				// A1: re-panic so net/http aborts the connection cleanly.
				// We use errors.Is via a type assertion because rec is type
				// any; the stdlib sentinel is not a wrapped error but errors.Is
				// is still correct and satisfies errorlint.
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}
				log.From(r.Context()).Error("panic recovered",
					"panic", rec, "path", r.URL.Path, "stack", string(debug.Stack()))
				// A2: only write the 500 if nothing has been sent yet.
				if !cw.wroteHeader {
					httpx.Error(cw, http.StatusInternalServerError, "internal server error")
				}
			}
		}()
		next.ServeHTTP(cw, r)
	})
}

// AccessLog logs one structured line per request after it completes:
// method, path, status, bytes, duration_ms, and request_id.
// It should be placed after RequestID in the middleware chain so the id is
// available in the context.
func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		cw := &capturingWriter{ResponseWriter: w}
		next.ServeHTTP(cw, r)
		elapsed := time.Since(start)
		ctx := r.Context()
		log.From(ctx).Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", cw.Status(),
			"bytes", cw.BytesWritten(),
			"duration_ms", elapsed.Milliseconds(),
			"request_id", RequestIDFromContext(ctx),
		)
	})
}

// MaxBytes caps request body size using http.MaxBytesReader.
func MaxBytes(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Timeout wraps the handler with http.TimeoutHandler, writing a 503 with the
// message "request timeout" when the handler does not complete within d.
func Timeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.TimeoutHandler(next, d, "request timeout")
	}
}

// newID returns a random 32-character lowercase hex string. On crypto/rand
// failure it falls back to a time-based value so ids remain unique.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
	}
	return hex.EncodeToString(b[:])
}
