package gateway_test

// Keycloak integration test — requires Docker. Skipped under -short.
//
// This test spins up a real Keycloak 26.0 container via a generic testcontainers
// container (the testcontainers-go keycloak module does not exist at the version
// currently in use), imports the app realm from deploy/keycloak/realm-export.json,
// starts the gateway with auth ENABLED pointing at the live JWKS endpoint, obtains
// a real access token for the demo/demo user, and asserts the full auth path:
//
//   - POST /v1/orders with a valid Keycloak token → 202 (demo user has "user" role).
//   - POST /v1/orders with no Authorization header → 401.
//   - POST /v1/orders with a garbage token → 401.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	gateway "go-boilerplate/examples/gateway"
)

// TestGateway_KeycloakRealToken is a full Keycloak integration test.
// It starts a real Keycloak container, imports the app realm, starts the gateway
// with auth enabled pointing at the live JWKS, obtains a real token, and asserts:
//   - Valid token → 202
//   - No token    → 401
//   - Garbage     → 401
func TestGateway_KeycloakRealToken(t *testing.T) {
	if testing.Short() {
		t.Skip("keycloak integration test requires Docker")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ── Start Keycloak container ─────────────────────────────────────────────
	// The testcontainers-go keycloak module does not exist at v0.42.0, so we
	// use a generic container with the realm file mounted via ContainerFile.
	kc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "quay.io/keycloak/keycloak:26.0",
			Cmd:          []string{"start-dev", "--import-realm"},
			ExposedPorts: []string{"8080/tcp"},
			Env: map[string]string{
				"KEYCLOAK_ADMIN":          "admin",
				"KEYCLOAK_ADMIN_PASSWORD": "admin",
				"KC_DB":                   "dev-mem",
				"KC_HEALTH_ENABLED":       "true",
			},
			Files: []testcontainers.ContainerFile{
				{
					HostFilePath:      "../../deploy/keycloak/realm-export.json",
					ContainerFilePath: "/opt/keycloak/data/import/realm-export.json",
					FileMode:          0o644,
				},
			},
			WaitingFor: wait.ForHTTP("/realms/app/.well-known/openid-configuration").
				WithPort("8080/tcp").
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err, "failed to start Keycloak container")
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	// ── Derive realm URL ────────────────────────────────────────────────────
	host, err := kc.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := kc.MappedPort(ctx, "8080/tcp")
	require.NoError(t, err)

	baseURL := fmt.Sprintf("http://%s:%s/realms/app", host, mappedPort.Port())
	jwksURL := baseURL + "/protocol/openid-connect/certs"
	tokenURL := baseURL + "/protocol/openid-connect/token"

	t.Logf("Keycloak base URL: %s", baseURL)

	// ── Obtain a real access token (demo/demo) ───────────────────────────────
	accessToken := fetchKeycloakToken(t, tokenURL)
	t.Logf("obtained access token (length=%d)", len(accessToken))

	// ── Start the gateway with auth enabled ──────────────────────────────────
	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	configureTopics(t)
	t.Setenv("PG_DSN", dsn)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "gateway-kc-test-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "false")
	t.Setenv("GATEWAY_JWKS_URL", jwksURL)
	t.Setenv("GATEWAY_JWKS_ISSUER", baseURL)
	t.Setenv("GATEWAY_JWKS_AUDIENCE", "gateway")
	t.Setenv("LOG_LEVEL", "error")

	appCtx := context.Background()
	a, err := gateway.NewApp(appCtx)
	require.NoError(t, err, "NewApp with real Keycloak JWKS must succeed")

	a.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Stop(stopCtx)
	})

	appBaseURL := "http://" + a.Addr()

	orderBody := map[string]interface{}{
		"customer_id":  "cust-kc-test",
		"amount_cents": int64(1234),
		"currency":     "USD",
	}
	bodyBytes, _ := json.Marshal(orderBody)

	// ── Assertion 1: No Authorization header → 401 ──────────────────────────
	resp, err := http.Post(appBaseURL+"/v1/orders", "application/json", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"POST /v1/orders without Authorization must return 401")

	// ── Assertion 2: Garbage token → 401 ────────────────────────────────────
	req, err := http.NewRequest(http.MethodPost, appBaseURL+"/v1/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer this.is.garbage")

	respGarbage, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	respGarbage.Body.Close()
	require.Equal(t, http.StatusUnauthorized, respGarbage.StatusCode,
		"POST /v1/orders with garbage token must return 401")

	// ── Assertion 3: Valid Keycloak token for demo user (role=user) → 202 ───
	req2, err := http.NewRequest(http.MethodPost, appBaseURL+"/v1/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer "+accessToken)

	respOK, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer respOK.Body.Close()
	bodyResp, _ := io.ReadAll(respOK.Body)
	require.Equal(t, http.StatusAccepted, respOK.StatusCode,
		"POST /v1/orders with valid Keycloak token (demo user, role=user) must return 202; body: %s", string(bodyResp))

	// ── Assertion 4: M2M client-credentials token (service account) → 202 ───
	// The gateway-m2m service account carries the realm role "user" and the
	// same audience mapper as the interactive client, so a token obtained
	// WITHOUT any user must pass verification and RBAC identically.
	m2mToken := fetchKeycloakM2MToken(t, tokenURL)
	req3, err := http.NewRequest(http.MethodPost, appBaseURL+"/v1/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Authorization", "Bearer "+m2mToken)

	respM2M, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer respM2M.Body.Close()
	bodyM2M, _ := io.ReadAll(respM2M.Body)
	require.Equal(t, http.StatusAccepted, respM2M.StatusCode,
		"POST /v1/orders with client-credentials token (gateway-m2m service account, role=user) must return 202; body: %s", string(bodyM2M))
}

// fetchKeycloakToken obtains an access token for demo/demo via the password grant.
func fetchKeycloakToken(t *testing.T, tokenURL string) string {
	t.Helper()

	form := url.Values{}
	form.Set("client_id", "gateway")
	form.Set("username", "demo")
	form.Set("password", "demo")
	form.Set("grant_type", "password")

	return postTokenForm(t, tokenURL, form)
}

// fetchKeycloakM2MToken obtains an access token for the gateway-m2m service
// account via the client-credentials grant (no user involved).
func fetchKeycloakM2MToken(t *testing.T, tokenURL string) string {
	t.Helper()

	form := url.Values{}
	form.Set("client_id", "gateway-m2m")
	form.Set("client_secret", "gateway-m2m-dev-secret")
	form.Set("grant_type", "client_credentials")

	return postTokenForm(t, tokenURL, form)
}

// postTokenForm POSTs an OAuth2 token request and returns the access token.
func postTokenForm(t *testing.T, tokenURL string, form url.Values) string {
	t.Helper()

	resp, err := http.Post(tokenURL, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	require.NoError(t, err, "token endpoint POST failed")
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"token endpoint returned non-200; body: %s", string(body))

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(body, &tokenResp), "failed to parse token response: %s", string(body))
	require.NotEmpty(t, tokenResp.AccessToken, "access_token missing in response: %s", string(body))

	return tokenResp.AccessToken
}
