package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/auth"
)

// ─── Test helpers ────────────────────────────────────────────────────────────

// testKeys holds an RSA key pair together with the JWK-wrapped public key.
type testKeys struct {
	priv   *rsa.PrivateKey
	pubJWK jwk.Key
}

func generateTestKeys(t *testing.T) testKeys {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pubJWK, err := jwk.FromRaw(priv.Public())
	require.NoError(t, err)
	require.NoError(t, pubJWK.Set(jwk.KeyIDKey, "test-key-1"))
	require.NoError(t, pubJWK.Set(jwk.AlgorithmKey, jwa.RS256))

	return testKeys{priv: priv, pubJWK: pubJWK}
}

// startJWKSServer creates an httptest.Server that serves the public key as a
// JWKS JSON payload and returns the server together with its URL.
func startJWKSServer(t *testing.T, keys testKeys) *httptest.Server {
	t.Helper()
	set := jwk.NewSet()
	require.NoError(t, set.AddKey(keys.pubJWK))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Cache-Control so jwx cache accepts the response.
		w.Header().Set("Cache-Control", "max-age=600")
		if err := json.NewEncoder(w).Encode(set); err != nil {
			http.Error(w, "encoding error", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

const (
	testIssuer   = "https://issuer.example.com"
	testAudience = "test-app"
	testSubject  = "user-123"
	testUsername = "jdoe"
)

// signToken builds and signs a JWT with the supplied private key.
// Pass a zero time.Time for exp to omit (or override inside claims directly).
func signToken(
	t *testing.T,
	keys testKeys,
	issuer, audience, subject, username string,
	exp time.Time,
	extraClaims map[string]any,
) []byte {
	t.Helper()

	b := jwt.NewBuilder().
		Issuer(issuer).
		Audience([]string{audience}).
		Subject(subject).
		IssuedAt(time.Now()).
		Expiration(exp).
		Claim("preferred_username", username).
		Claim("realm_access", map[string]any{
			"roles": []any{"admin", "user"},
		})

	for k, v := range extraClaims {
		b = b.Claim(k, v)
	}

	tok, err := b.Build()
	require.NoError(t, err)

	// Sign using the private key; kid is propagated from the JWK.
	privJWK, err := jwk.FromRaw(keys.priv)
	require.NoError(t, err)
	require.NoError(t, privJWK.Set(jwk.KeyIDKey, "test-key-1"))
	require.NoError(t, privJWK.Set(jwk.AlgorithmKey, jwa.RS256))

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, privJWK))
	require.NoError(t, err)
	return signed
}

// newVerifier creates a JWKSVerifier pointing at the given JWKS server URL.
func newVerifier(t *testing.T, jwksURL, issuer, audience string, opts ...auth.Option) *auth.JWKSVerifier {
	t.Helper()
	ctx := context.Background()
	v, err := auth.NewJWKSVerifier(ctx, jwksURL, issuer, audience, opts...)
	require.NoError(t, err)
	return v
}

// ─── JWKSVerifier tests ───────────────────────────────────────────────────────

func TestJWKSVerifier_ValidToken(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)

	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	signed := signToken(t, keys, testIssuer, testAudience, testSubject, testUsername,
		time.Now().Add(time.Hour), nil)

	p, err := v.Verify(context.Background(), string(signed))
	require.NoError(t, err)

	assert.Equal(t, testSubject, p.Subject)
	assert.Equal(t, testUsername, p.Username)
	assert.Contains(t, p.Roles, "admin")
	assert.Contains(t, p.Roles, "user")
	assert.Len(t, p.Roles, 2)
}

func TestJWKSVerifier_Expired(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	signed := signToken(t, keys, testIssuer, testAudience, testSubject, testUsername,
		time.Now().Add(-time.Hour), nil) // exp in the past

	_, err := v.Verify(context.Background(), string(signed))
	require.Error(t, err)
	assert.True(t, errors.Is(err, auth.ErrInvalidToken), "expected ErrInvalidToken, got: %v", err)
}

func TestJWKSVerifier_WrongIssuer(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	// Token signed with a different issuer.
	signed := signToken(t, keys, "https://evil.example.com", testAudience, testSubject, testUsername,
		time.Now().Add(time.Hour), nil)

	_, err := v.Verify(context.Background(), string(signed))
	require.Error(t, err)
	assert.True(t, errors.Is(err, auth.ErrInvalidToken))
}

func TestJWKSVerifier_WrongAudience(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	signed := signToken(t, keys, testIssuer, "wrong-app", testSubject, testUsername,
		time.Now().Add(time.Hour), nil)

	_, err := v.Verify(context.Background(), string(signed))
	require.Error(t, err)
	assert.True(t, errors.Is(err, auth.ErrInvalidToken))
}

func TestJWKSVerifier_BadSignature(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	signed := signToken(t, keys, testIssuer, testAudience, testSubject, testUsername,
		time.Now().Add(time.Hour), nil)

	// Tamper with the last character of the signature segment.
	tampered := []byte(string(signed))
	last := len(tampered) - 1
	if tampered[last] == 'a' {
		tampered[last] = 'b'
	} else {
		tampered[last] = 'a'
	}

	_, err := v.Verify(context.Background(), string(tampered))
	require.Error(t, err)
	assert.True(t, errors.Is(err, auth.ErrInvalidToken))
}

func TestJWKSVerifier_CustomRolesClaimPath(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience,
		auth.WithRolesClaimPath("custom_access.roles"))

	signed := signToken(t, keys, testIssuer, testAudience, testSubject, testUsername,
		time.Now().Add(time.Hour),
		map[string]any{
			"custom_access": map[string]any{
				"roles": []any{"superadmin"},
			},
		},
	)

	p, err := v.Verify(context.Background(), string(signed))
	require.NoError(t, err)
	assert.Contains(t, p.Roles, "superadmin")
}

// ─── Middleware tests ─────────────────────────────────────────────────────────

// echoSubjectHandler is a simple handler that writes the subject from the
// Principal in context. Used to confirm the principal was stored correctly.
func echoSubjectHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := auth.From(r.Context())
		if !ok {
			http.Error(w, "no principal", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(p.Subject))
	})
}

func TestMiddleware_ValidBearerSetsPrincipal(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	signed := signToken(t, keys, testIssuer, testAudience, testSubject, testUsername,
		time.Now().Add(time.Hour), nil)

	handler := auth.Middleware(v)(echoSubjectHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+string(signed))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, testSubject, string(body))
}

func TestMiddleware_MissingHeader_Returns401(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	handler := auth.Middleware(v)(echoSubjectHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil) // no Authorization header
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))

	var prob map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&prob))
	assert.EqualValues(t, http.StatusUnauthorized, prob["status"])
}

func TestMiddleware_InvalidToken_Returns401(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	handler := auth.Middleware(v)(echoSubjectHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer not.a.real.jwt")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
}

func TestMiddleware_MalformedBearer_Returns401(t *testing.T) {
	keys := generateTestKeys(t)
	srv := startJWKSServer(t, keys)
	v := newVerifier(t, srv.URL, testIssuer, testAudience)

	handler := auth.Middleware(v)(echoSubjectHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Basic user:pass") // wrong scheme
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestRequireRole_AllowsMatchingRole(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	handler := auth.RequireRole("admin")(inner)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := auth.Into(req.Context(), auth.Principal{
		Subject: "u1",
		Roles:   []string{"admin", "user"},
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req.WithContext(ctx))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, called)
}

func TestRequireRole_ForbiddenOnMissingRole(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := auth.RequireRole("superadmin")(inner)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := auth.Into(req.Context(), auth.Principal{
		Subject: "u1",
		Roles:   []string{"user"},
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req.WithContext(ctx))

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
}

func TestRequireRole_UnauthorizedOnNoPrincipal(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := auth.RequireRole("admin")(inner)

	req := httptest.NewRequest(http.MethodGet, "/", nil) // no principal in ctx
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
