// Package blob provides an object-storage abstraction backed by any
// S3-compatible store (SeaweedFS locally, AWS S3 in prod).
package blob

import (
	"context"
	"io"
	"time"
)

// ObjectStore is a minimal, S3-compatible object-storage interface.
type ObjectStore interface {
	// Put uploads r (of known size) under key with the given content-type.
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	// Get downloads the object at key; caller must close the returned reader.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete removes the object at key.
	Delete(ctx context.Context, key string) error
	// Exists reports whether the object at key is present. A missing object
	// is not an error — only unexpected storage errors are returned.
	Exists(ctx context.Context, key string) (bool, error)
	// PresignGet returns a time-limited pre-signed GET URL for key.
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
	// List returns all object keys whose name starts with prefix.
	List(ctx context.Context, prefix string) ([]string, error)
}
