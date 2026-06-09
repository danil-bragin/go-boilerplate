// Package mockhttp provides httptest-based mock HTTP servers for use in tests.
//
// It exposes three building blocks:
//
//   - [JWKSServer] – an RSA-2048 JWKS endpoint that can also mint signed JWTs,
//     making it easy to drive a [platform/auth.JWKSVerifier] in tests.
//   - [Recorder] + [Server] – a recording wrapper around any [http.Handler]
//     that captures each incoming request for assertion.
//   - [JSON] – a one-liner factory for simple JSON response handlers.
package mockhttp

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// ─── JWKS server ─────────────────────────────────────────────────────────────

// JWKSServer is an httptest.Server that serves a single RSA-2048 public key as
// a JWKS JSON document and can mint RS256 JWTs signed by the corresponding
// private key.
type JWKSServer struct {
	srv     *httptest.Server
	privJWK jwk.Key // private key wrapped as a JWK (for signing)
}

const jwksKid = "testkit-key-1"

// JWKS starts a new JWKS server backed by a freshly generated RSA-2048 key
// pair and registers t.Cleanup to close it.
//
// The server responds to all paths (including "/") with a JSON JWKS containing
// the public key annotated with kid="testkit-key-1" and alg=RS256.
func JWKS(t *testing.T) *JWKSServer {
	t.Helper()

	// Generate the RSA key pair.
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("mockhttp.JWKS: generate RSA key: %v", err)
	}

	// Wrap the public key as a JWK with kid + alg.
	pubJWK, err := jwk.Import(priv.Public())
	if err != nil {
		t.Fatalf("mockhttp.JWKS: wrap public key: %v", err)
	}
	if err := pubJWK.Set(jwk.KeyIDKey, jwksKid); err != nil {
		t.Fatalf("mockhttp.JWKS: set kid: %v", err)
	}
	if err := pubJWK.Set(jwk.AlgorithmKey, jwa.RS256()); err != nil {
		t.Fatalf("mockhttp.JWKS: set alg: %v", err)
	}

	// Wrap the private key as a JWK with kid + alg (needed for signing).
	privJWK, err := jwk.Import(priv)
	if err != nil {
		t.Fatalf("mockhttp.JWKS: wrap private key: %v", err)
	}
	if err := privJWK.Set(jwk.KeyIDKey, jwksKid); err != nil {
		t.Fatalf("mockhttp.JWKS: set kid on privJWK: %v", err)
	}
	if err := privJWK.Set(jwk.AlgorithmKey, jwa.RS256()); err != nil {
		t.Fatalf("mockhttp.JWKS: set alg on privJWK: %v", err)
	}

	// Build the JWKS payload once.
	set := jwk.NewSet()
	if err := set.AddKey(pubJWK); err != nil {
		t.Fatalf("mockhttp.JWKS: add key to set: %v", err)
	}
	payload, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("mockhttp.JWKS: marshal JWKS: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// jwx's cache respects Cache-Control; set a reasonable max-age so the
		// verifier does not attempt to re-fetch the keys mid-test.
		w.Header().Set("Cache-Control", "max-age=600")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	return &JWKSServer{srv: srv, privJWK: privJWK}
}

// URL returns the base URL of the JWKS server (e.g. "http://127.0.0.1:PORT").
// Pass this directly to auth.NewJWKSVerifier.
func (j *JWKSServer) URL() string { return j.srv.URL }

// Sign builds and signs a compact RS256 JWT.
//
// A default expiration of now+1 hour is applied before the caller's claims are
// merged, so tests that only care about a valid token can omit "exp". To
// override the expiration pass "exp" as a [time.Time] in claims:
//
//	js.Sign(map[string]any{"iss": "...", "exp": time.Now().Add(-time.Hour)})
//
// Every key in claims is set on the token via jwt.Builder.Claim, which covers
// both standard claims (iss, sub, aud, exp, iat, nbf, jti) and private ones.
// "aud" must be a string or []string; "exp"/"iat"/"nbf" must be time.Time.
func (j *JWKSServer) Sign(claims map[string]any) string {
	b := jwt.NewBuilder().
		Expiration(time.Now().Add(time.Hour)) // sensible default; caller may override

	for k, v := range claims {
		b = b.Claim(k, v)
	}

	tok, err := b.Build()
	if err != nil {
		panic("mockhttp.JWKSServer.Sign: build token: " + err.Error())
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256(), j.privJWK))
	if err != nil {
		panic("mockhttp.JWKSServer.Sign: sign token: " + err.Error())
	}
	return string(signed)
}

// ─── Recording server ─────────────────────────────────────────────────────────

// RecordedRequest captures the method, path, and body of a single HTTP request.
type RecordedRequest struct {
	Method string
	Path   string
	Body   []byte
}

// Recorder wraps an httptest.Server with a middleware that records every
// incoming request before delegating to the underlying handler.
type Recorder struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []RecordedRequest
}

// Server creates a new recording server that delegates to handler. It registers
// t.Cleanup to close the server.
func Server(t *testing.T, handler http.Handler) *Recorder {
	t.Helper()

	r := &Recorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		// Restore body so the wrapped handler can read it too.
		req.Body = io.NopCloser(bytes.NewReader(body))

		r.mu.Lock()
		r.reqs = append(r.reqs, RecordedRequest{
			Method: req.Method,
			Path:   req.URL.Path,
			Body:   body,
		})
		r.mu.Unlock()

		handler.ServeHTTP(w, req)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

// URL returns the base URL of the recording server.
func (r *Recorder) URL() string { return r.srv.URL }

// Requests returns a copy of all recorded requests in arrival order.
func (r *Recorder) Requests() []RecordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RecordedRequest, len(r.reqs))
	copy(out, r.reqs)
	return out
}

// ─── JSON handler helper ──────────────────────────────────────────────────────

// JSON returns an [http.HandlerFunc] that writes body marshalled as JSON with
// the given HTTP status code and a "Content-Type: application/json" header.
// It panics if body cannot be marshalled — callers should only pass static,
// marshal-safe values.
func JSON(status int, body any) http.HandlerFunc {
	payload, err := json.Marshal(body)
	if err != nil {
		panic("mockhttp.JSON: marshal body: " + err.Error())
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(payload)
	}
}
