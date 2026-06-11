package auth_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/testkit/mockhttp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJWKSVerifier_HTTPSEnforcement covers the AUTH_ALLOW_INSECURE_JWKS rule:
//
//   - a plaintext http:// JWKS URL is REJECTED at construction by default
//     (fail closed), before any network fetch;
//   - WithAllowInsecureJWKS(true) lets the same http URL through (dev escape
//     hatch) and the verifier builds normally against the mock;
//   - an https:// URL clears the guard regardless of the flag (the only
//     remaining failure is the network fetch, not the scheme check).
func TestJWKSVerifier_HTTPSEnforcement(t *testing.T) {
	t.Run("http rejected by default before fetch", func(t *testing.T) {
		// Point at an unreachable http host: if the guard did NOT fire first,
		// the error would be a fetch/timeout error, not the scheme error.
		_, err := auth.NewJWKSVerifier(
			context.Background(),
			"http://192.0.2.1:65535/jwks", // TEST-NET-1, unroutable
			testIssuer, testAudience,
		)
		require.Error(t, err)
		assert.True(t, errors.Is(err, auth.ErrInvalidToken))
		assert.Contains(t, err.Error(), "https", "error must name the https requirement")
	})

	t.Run("http allowed with flag", func(t *testing.T) {
		js := mockhttp.JWKS(t)
		require.True(t, strings.HasPrefix(js.URL(), "http://"), "mock JWKS is plaintext http")

		v, err := auth.NewJWKSVerifier(
			context.Background(),
			js.URL(), testIssuer, testAudience,
			auth.WithAllowInsecureJWKS(true),
		)
		require.NoError(t, err, "http URL must be accepted when the flag is set")
		require.NotNil(t, v)
	})

	t.Run("https clears the scheme guard", func(t *testing.T) {
		// Unreachable https endpoint: the construction can only fail on the
		// FETCH now, never on the scheme guard. Assert the error (if any) is
		// not the https-scheme rejection.
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := auth.NewJWKSVerifier(
			ctx,
			"https://192.0.2.1:65535/jwks",
			testIssuer, testAudience,
		)
		// We expect a fetch error here (host unroutable), but crucially NOT
		// the scheme-guard error.
		if err != nil {
			assert.NotContains(t, err.Error(), "must use https",
				"https URL must clear the scheme guard")
		}
	})
}
