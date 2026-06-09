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
	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/blob"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

// newMinioStore starts a MinIO testcontainer and returns a real blob.MinioStore
// wired to it. The container is terminated automatically when the test ends.
func newMinioStore(t *testing.T) *blob.MinioStore {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (minio container)")
	}
	ctx := context.Background()

	ctr, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	endpoint, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)

	cfg := blob.Config{
		Endpoint:  endpoint,
		AccessKey: ctr.Username,
		SecretKey: ctr.Password,
		Bucket:    "testbucket",
		UseSSL:    false,
		Region:    "us-east-1",
	}

	store, err := blob.New(ctx, cfg)
	require.NoError(t, err)
	return store
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
// presigned URL, verifying the full MinIO round-trip.
func TestIntegration_AttachmentRoundTrip(t *testing.T) {
	store := newMinioStore(t)

	h := attachments.New(store, flagOn)
	r := chi.NewRouter()
	h.Mount(r)

	// --- Upload ---
	content := []byte("integration test content")
	req := httptest.NewRequest(http.MethodPost, "/orders/"+integrationOrderID+"/attachment", bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Filename", "report.txt")
	req = withIntegrationPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusCreated, rw.Code, "upload should return 201")

	// --- Download route → 302 redirect ---
	req2 := httptest.NewRequest(http.MethodGet, "/orders/"+integrationOrderID+"/attachment/report.txt", http.NoBody)
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
