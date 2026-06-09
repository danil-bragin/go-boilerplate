package mockhttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/testkit/mockhttp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJWKS_MintedTokenVerifies confirms that a token minted by JWKSServer.Sign
// is accepted by a platform/auth.JWKSVerifier pointed at the same server.
func TestJWKS_MintedTokenVerifies(t *testing.T) {
	const (
		issuer   = "https://issuer.example.test"
		audience = "aud1"
		subject  = "u1"
	)

	js := mockhttp.JWKS(t)

	token := js.Sign(map[string]any{
		"iss": issuer,
		"aud": audience,
		"sub": subject,
	})
	require.NotEmpty(t, token)
	assert.True(t, strings.Count(token, ".") == 2, "compact JWT should have exactly two dots")

	// Verify via the real auth.JWKSVerifier — this proves end-to-end integration.
	ctx := context.Background()
	v, err := auth.NewJWKSVerifier(ctx, js.URL(), issuer, audience)
	require.NoError(t, err)

	p, err := v.Verify(ctx, token)
	require.NoError(t, err)
	assert.Equal(t, subject, p.Subject)
}

// TestJWKS_ExpiredTokenRejected confirms that Sign respects an "exp" override
// in the past and that the verifier rejects the resulting token.
func TestJWKS_ExpiredTokenRejected(t *testing.T) {
	const (
		issuer   = "https://issuer.example.test"
		audience = "aud1"
		subject  = "u2"
	)

	js := mockhttp.JWKS(t)

	// Override exp to be one hour in the past.
	expiredToken := js.Sign(map[string]any{
		"iss": issuer,
		"aud": audience,
		"sub": subject,
		"exp": time.Now().Add(-time.Hour),
	})

	ctx := context.Background()
	v, err := auth.NewJWKSVerifier(ctx, js.URL(), issuer, audience)
	require.NoError(t, err)

	_, err = v.Verify(ctx, expiredToken)
	require.Error(t, err)
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
}

// TestServer_RecordsRequests confirms that Recorder captures method, path, and
// body, and that the underlying handler still returns the expected response.
func TestServer_RecordsRequests(t *testing.T) {
	rec := mockhttp.Server(t, mockhttp.JSON(200, map[string]string{"ok": "yes"}))

	reqBody := `{"hello":"world"}`
	resp, err := http.Post(rec.URL()+"/x", "application/json", bytes.NewBufferString(reqBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	// Verify the response.
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var got map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, map[string]string{"ok": "yes"}, got)

	// Verify the recorded request.
	reqs := rec.Requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, http.MethodPost, reqs[0].Method)
	assert.Equal(t, "/x", reqs[0].Path)
	assert.JSONEq(t, reqBody, string(reqs[0].Body))
}

// TestServer_MultipleRequests checks that all sequential requests are recorded.
func TestServer_MultipleRequests(t *testing.T) {
	rec := mockhttp.Server(t, mockhttp.JSON(204, nil))

	for i := 0; i < 3; i++ {
		resp, err := http.Get(rec.URL() + "/ping")
		require.NoError(t, err)
		resp.Body.Close()
	}

	assert.Len(t, rec.Requests(), 3)
}

// TestJSON_WritesCorrectStatusAndBody is a unit-level check for the JSON helper.
func TestJSON_WritesCorrectStatusAndBody(t *testing.T) {
	h := mockhttp.JSON(http.StatusCreated, map[string]int{"n": 42})

	rec := mockhttp.Server(t, h)
	resp, err := http.Get(rec.URL() + "/")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var got map[string]int
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, 42, got["n"])
}
