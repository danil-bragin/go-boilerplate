package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/testkit/mockhttp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// futureToken mints a token whose iat/nbf are 15s in the future — the shape
// produced by an issuer whose clock runs slightly ahead of the verifier's.
func futureToken(js *mockhttp.JWKSServer) string {
	future := time.Now().Add(15 * time.Second)
	return js.Sign(map[string]any{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": testSubject,
		"iat": future,
		"nbf": future,
		"exp": time.Now().Add(time.Hour),
	})
}

// TestJWKSVerifier_ClockSkew_AcceptsFutureToken: with a 30s acceptable skew a
// token minted 15s "in the future" (issuer clock ahead) must verify.
func TestJWKSVerifier_ClockSkew_AcceptsFutureToken(t *testing.T) {
	js := mockhttp.JWKS(t)
	v, err := auth.NewJWKSVerifier(context.Background(), js.URL(), testIssuer, testAudience, auth.WithAllowInsecureJWKS(true),
		auth.WithClockSkew(30*time.Second))
	require.NoError(t, err)

	p, err := v.Verify(context.Background(), futureToken(js))
	require.NoError(t, err, "15s-future token must pass with 30s skew")
	assert.Equal(t, testSubject, p.Subject)
}

// TestJWKSVerifier_ClockSkew_ZeroRejectsFutureToken: without skew the same
// future-dated token must be rejected (nbf not yet valid).
func TestJWKSVerifier_ClockSkew_ZeroRejectsFutureToken(t *testing.T) {
	js := mockhttp.JWKS(t)
	v, err := auth.NewJWKSVerifier(context.Background(), js.URL(), testIssuer, testAudience, auth.WithAllowInsecureJWKS(true))
	require.NoError(t, err)

	_, err = v.Verify(context.Background(), futureToken(js))
	require.Error(t, err, "15s-future token must fail with zero skew")
	assert.True(t, errors.Is(err, auth.ErrInvalidToken))
}

// TestJWKSVerifier_AZP_MismatchRejected: when WithRequiredAZP is set, a token
// issued to a different client (azp claim differs) must be rejected even
// though issuer/audience/signature are all valid.
func TestJWKSVerifier_AZP_MismatchRejected(t *testing.T) {
	js := mockhttp.JWKS(t)
	v, err := auth.NewJWKSVerifier(context.Background(), js.URL(), testIssuer, testAudience, auth.WithAllowInsecureJWKS(true),
		auth.WithRequiredAZP("gateway"))
	require.NoError(t, err)

	tok := js.Sign(map[string]any{
		"iss": testIssuer, "aud": testAudience, "sub": testSubject,
		"azp": "other-client",
	})
	_, err = v.Verify(context.Background(), tok)
	require.Error(t, err)
	assert.True(t, errors.Is(err, auth.ErrInvalidToken))

	// Token without any azp claim must also be rejected when azp is required.
	tokNoAZP := js.Sign(map[string]any{
		"iss": testIssuer, "aud": testAudience, "sub": testSubject,
	})
	_, err = v.Verify(context.Background(), tokNoAZP)
	require.Error(t, err, "token without azp must fail when AUTH_REQUIRED_AZP is set")
}

// TestJWKSVerifier_AZP_MatchAccepted: matching azp passes.
func TestJWKSVerifier_AZP_MatchAccepted(t *testing.T) {
	js := mockhttp.JWKS(t)
	v, err := auth.NewJWKSVerifier(context.Background(), js.URL(), testIssuer, testAudience, auth.WithAllowInsecureJWKS(true),
		auth.WithRequiredAZP("gateway"))
	require.NoError(t, err)

	tok := js.Sign(map[string]any{
		"iss": testIssuer, "aud": testAudience, "sub": testSubject,
		"azp": "gateway",
	})
	p, err := v.Verify(context.Background(), tok)
	require.NoError(t, err)
	assert.Equal(t, testSubject, p.Subject)
}
