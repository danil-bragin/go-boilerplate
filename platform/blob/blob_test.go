package blob_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"go-boilerplate/platform/blob"
)

// newStore starts a MinIO testcontainer and returns a MinioStore wired to it.
// The container is terminated automatically when the test ends.
func newStore(t *testing.T) *blob.MinioStore {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	endpoint, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)

	cfg := blob.Config{
		Endpoint:  endpoint,
		AccessKey: ctr.Username, // "minioadmin" (default)
		SecretKey: ctr.Password, // "minioadmin" (default)
		Bucket:    "testbucket",
		UseSSL:    false,
		Region:    "us-east-1",
	}

	store, err := blob.New(ctx, cfg)
	require.NoError(t, err)
	return store
}

func TestMinio_PutGetRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStore(t)

	content := []byte("hello")
	err := store.Put(ctx, "a/b.txt", bytes.NewReader(content), int64(len(content)), "text/plain")
	require.NoError(t, err)

	rc, err := store.Get(ctx, "a/b.txt")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

func TestMinio_Exists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStore(t)

	// Object not yet uploaded.
	ok, err := store.Exists(ctx, "a/b.txt")
	require.NoError(t, err)
	require.False(t, ok)

	// Upload and check again.
	body := []byte("data")
	require.NoError(t, store.Put(ctx, "a/b.txt", bytes.NewReader(body), int64(len(body)), "application/octet-stream"))

	ok, err = store.Exists(ctx, "a/b.txt")
	require.NoError(t, err)
	require.True(t, ok)

	// A completely different key should return false without error.
	ok, err = store.Exists(ctx, "nope")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestMinio_Delete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStore(t)

	body := []byte("to-be-deleted")
	require.NoError(t, store.Put(ctx, "a/b.txt", bytes.NewReader(body), int64(len(body)), "text/plain"))

	ok, err := store.Exists(ctx, "a/b.txt")
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, store.Delete(ctx, "a/b.txt"))

	ok, err = store.Exists(ctx, "a/b.txt")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestMinio_ListByPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStore(t)

	for key, data := range map[string][]byte{
		"p/1": []byte("one"),
		"p/2": []byte("two"),
		"q/3": []byte("three"),
	} {
		require.NoError(t, store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "text/plain"))
	}

	keys, err := store.List(ctx, "p/")
	require.NoError(t, err)

	sort.Strings(keys)
	require.Equal(t, []string{"p/1", "p/2"}, keys)

	for _, k := range keys {
		require.NotEqual(t, "q/3", k, "List('p/') must not include q/3")
	}
}

func TestMinio_PresignGetDownloads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStore(t)

	content := []byte("presigned-content")
	require.NoError(t, store.Put(ctx, "signed/obj", bytes.NewReader(content), int64(len(content)), "text/plain"))

	rawURL, err := store.PresignGet(ctx, "signed/obj", 5*time.Minute)
	require.NoError(t, err)
	require.NotEmpty(t, rawURL)

	// The presigned URL host is the container endpoint, reachable from the
	// test host. Perform a plain HTTP GET and verify the response body.
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(rawURL) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, content, got)
}
