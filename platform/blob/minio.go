package blob

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Compile-time assertion that *MinioStore satisfies ObjectStore.
var _ ObjectStore = (*MinioStore)(nil)

// MinioStore implements ObjectStore using the MinIO Go SDK, which is
// compatible with MinIO and AWS S3.
type MinioStore struct {
	client *minio.Client
	bucket string
	region string
}

// New creates a MinioStore from cfg, connects to the endpoint, and ensures the
// configured bucket exists (creating it if necessary).
func New(ctx context.Context, cfg Config) (*MinioStore, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, err
	}

	s := &MinioStore{client: client, bucket: cfg.Bucket, region: cfg.Region}

	// Ensure the bucket exists.
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{
			Region: cfg.Region,
		}); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Put uploads data from r under key with the given size and content-type.
func (s *MinioStore) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	return err
}

// Get downloads the object identified by key.
// It calls Stat on the returned *minio.Object to surface a not-found error
// before the caller attempts to read — GetObject itself is lazy.
func (s *MinioStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// Eagerly stat so callers see a meaningful error rather than a read-time
	// "The specified key does not exist" error.
	if _, err = obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, err
	}
	return obj, nil
}

// Delete removes the object at key from the bucket.
func (s *MinioStore) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// Exists reports whether the object at key is present.
//
// MinIO returns error code "NoSuchKey" for a missing object. AWS S3 issues a
// HEAD request and returns HTTP 404 with code "NotFound" (no response body to
// parse). Both cases are treated as (false, nil). All other errors propagate.
//
// Note: a new container-level test for the S3 NotFound path is not included
// because the existing MinIO test container covers the NoSuchKey path and
// setting up a real-S3 container is outside the scope of this package's tests.
func (s *MinioStore) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" || resp.Code == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// PresignGet returns a time-limited pre-signed GET URL for key.
func (s *MinioStore) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, ttl, url.Values(nil))
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// List returns all object keys under the given prefix (recursive).
func (s *MinioStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// HealthCheck verifies that the configured bucket is reachable.
func (s *MinioStore) HealthCheck(ctx context.Context) error {
	_, err := s.client.BucketExists(ctx, s.bucket)
	return err
}
