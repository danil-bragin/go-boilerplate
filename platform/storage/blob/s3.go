package blob

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Compile-time assertion that *S3Store satisfies ObjectStore.
var _ ObjectStore = (*S3Store)(nil)

// S3Store implements ObjectStore using the official AWS SDK for Go v2, which
// works against AWS S3 and any S3-compatible store (SeaweedFS locally).
type S3Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
}

// New creates an S3Store from cfg, connects to the endpoint, and ensures the
// configured bucket exists (creating it if necessary).
func New(ctx context.Context, cfg Config) (*S3Store, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey.Reveal(), ""),
		),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.endpointURL())
		}
		o.UsePathStyle = cfg.UsePathStyle
		// Third-party S3 implementations (SeaweedFS, Ceph, …) do not all
		// support the SDK's default flexible-checksum trailers; only compute
		// and validate checksums when an operation requires them.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	s := &S3Store{
		client:  client,
		presign: s3.NewPresignClient(client),
		bucket:  cfg.Bucket,
	}

	// Ensure the bucket exists.
	exists, err := s.bucketExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(cfg.Bucket),
		}); err != nil {
			var owned *types.BucketAlreadyOwnedByYou
			if !errors.As(err, &owned) {
				return nil, err
			}
		}
	}

	return s, nil
}

// Put uploads data from r under key with the given size and content-type.
func (s *S3Store) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          r,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
	})
	return err
}

// Get downloads the object identified by key. GetObject is eager: a missing
// key surfaces a meaningful error here, before the caller attempts to read.
func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// Delete removes the object at key from the bucket.
func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

// Exists reports whether the object at key is present.
//
// HeadObject returns HTTP 404 for a missing object; AWS S3 reports it as
// "NotFound" (no response body to parse) while some S3-compatible stores
// answer "NoSuchKey". Both cases are treated as (false, nil). All other
// errors propagate.
func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// PresignGet returns a time-limited pre-signed GET URL for key.
func (s *S3Store) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

// List returns all object keys under the given prefix (recursive).
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

// HealthCheck verifies that the configured bucket is reachable.
func (s *S3Store) HealthCheck(ctx context.Context) error {
	_, err := s.bucketExists(ctx)
	return err
}

// bucketExists reports whether the configured bucket exists. A 404 is not an
// error — only unexpected storage errors are returned.
func (s *S3Store) bucketExists(ctx context.Context) (bool, error) {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// isNotFound reports whether err is an S3 "object/bucket does not exist"
// error: typed NotFound/NoSuchKey/NoSuchBucket, or any API error whose code
// says so (HEAD responses carry no body, so the SDK cannot always produce
// the typed error).
func isNotFound(err error) bool {
	var notFound *types.NotFound
	var noSuchKey *types.NoSuchKey
	var noSuchBucket *types.NoSuchBucket
	if errors.As(err, &notFound) || errors.As(err, &noSuchKey) || errors.As(err, &noSuchBucket) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return true
		}
	}
	return false
}
