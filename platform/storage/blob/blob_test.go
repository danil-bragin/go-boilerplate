package blob_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"

	"go-boilerplate/platform/storage/blob"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// seaweedImage is the SeaweedFS all-in-one image used for contract tests.
const seaweedImage = "chrislusf/seaweedfs:3.80"

// newStore starts a SeaweedFS testcontainer (S3 gateway on 8333), creates the
// test bucket via the AWS SDK, and returns an S3Store wired to it. The
// container is terminated automatically when the test ends.
func newStore(t *testing.T) *blob.S3Store {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (seaweedfs container)")
	}
	ctx := context.Background()

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        seaweedImage,
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

	createTestBucket(ctx, t, cfg)

	store, err := blob.New(ctx, cfg)
	require.NoError(t, err)
	return store
}

// createTestBucket creates cfg.Bucket via the AWS SDK, retrying until the
// SeaweedFS filer behind the S3 gateway is ready to serve requests (the S3
// port starts listening slightly before the filer accepts writes).
func createTestBucket(ctx context.Context, t *testing.T, cfg blob.Config) {
	t.Helper()

	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey.Reveal(), ""),
		),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + cfg.Endpoint)
		o.UsePathStyle = true
	})

	deadline := time.Now().Add(time.Minute)
	for {
		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)})
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if err == nil || errors.As(err, &owned) || errors.As(err, &exists) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("create test bucket %q: %v", cfg.Bucket, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestS3Store_PutGetRoundTrip(t *testing.T) {
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

func TestS3Store_GetMissingKeyFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStore(t)

	_, err := store.Get(ctx, "does/not/exist")
	require.Error(t, err, "Get on a missing key must surface an error eagerly")
}

func TestS3Store_Exists(t *testing.T) {
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

func TestS3Store_Delete(t *testing.T) {
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

func TestS3Store_ListByPrefix(t *testing.T) {
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

func TestS3Store_PresignGetDownloads(t *testing.T) {
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
