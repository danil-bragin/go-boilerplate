package attachments_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/blob"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newBlobStore starts a SeaweedFS testcontainer (S3 gateway on 8333) and
// returns a real blob.S3Store wired to it. The container is terminated
// automatically when the test ends.
func newBlobStore(t *testing.T) *blob.S3Store {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (seaweedfs container)")
	}
	ctx := context.Background()

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "chrislusf/seaweedfs:3.80",
			Cmd:          []string{"server", "-s3"},
			ExposedPorts: []string{"8333/tcp"},
			WaitingFor:   wait.ForListeningPort("8333/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	endpoint, err := ctr.PortEndpoint(ctx, "8333/tcp", "")
	require.NoError(t, err)

	cfg := blob.Config{
		Endpoint:     endpoint,
		AccessKey:    "test",
		SecretKey:    "test",
		Bucket:       "testbucket",
		UseSSL:       false,
		Region:       "us-east-1",
		UsePathStyle: true,
	}

	// blob.New ensures the bucket exists; retry briefly because the SeaweedFS
	// filer behind the S3 gateway becomes ready slightly after the port opens.
	deadline := time.Now().Add(time.Minute)
	for {
		store, err := blob.New(ctx, cfg)
		if err == nil {
			return store
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "blob.New against seaweedfs")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// integrationOrderID is a valid UUID used as the order identifier in the
// integration round-trip test.
const integrationOrderID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

// withIntegrationPrincipal injects a "user"-role principal into the request context.
func withIntegrationPrincipal(req *http.Request) *http.Request {
	ctx := auth.Into(req.Context(), auth.Principal{
		Subject: "integration-user",
		Roles:   []string{"user"},
	})
	return req.WithContext(ctx)
}

// TestIntegration_AttachmentRoundTrip uploads a file and downloads it via the
// presigned URL, verifying the full object-store round-trip.
func TestIntegration_AttachmentRoundTrip(t *testing.T) {
	store := newBlobStore(t)

	h := attachments.New(store, flagOn)
	r := chi.NewRouter()
	h.Mount(r)

	// --- Upload ---
	content := []byte("integration test content")
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+integrationOrderID+"/attachment", bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Filename", "report.txt")
	req = withIntegrationPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusCreated, rw.Code, "upload should return 201")

	// --- Download route → 302 redirect ---
	req2 := httptest.NewRequest(http.MethodGet, "/v1/orders/"+integrationOrderID+"/attachment/report.txt", http.NoBody)
	req2 = withIntegrationPrincipal(req2)
	rw2 := httptest.NewRecorder()
	r.ServeHTTP(rw2, req2)
	require.Equal(t, http.StatusFound, rw2.Code, "download should return 302")

	presignedURL := rw2.Header().Get("Location")
	require.NotEmpty(t, presignedURL, "Location header must contain the presigned URL")
	require.True(t, strings.HasPrefix(presignedURL, "http"), "presigned URL must be an absolute URL")

	// --- Fetch via the presigned URL → 200, same bytes ---
	client := &http.Client{
		Timeout: 15 * time.Second,
		// Do NOT follow redirects automatically — we want to verify the URL.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(presignedURL) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "GET on presigned URL must return 200")

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, content, got, "downloaded bytes must match uploaded bytes")
}
