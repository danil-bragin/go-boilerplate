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
	// PresignGet returns a time-limited pre-signed GET URL for key. Options
	// (e.g. PresignAttachment) adjust the response the store serves for the
	// signed request.
	PresignGet(ctx context.Context, key string, ttl time.Duration, opts ...PresignOption) (string, error)
	// List returns all object keys whose name starts with prefix.
	List(ctx context.Context, prefix string) ([]string, error)
}

// PresignOption customises the response a pre-signed GET URL elicits from the
// object store (e.g. forcing an attachment download disposition).
type PresignOption func(*presignConfig)

// presignConfig accumulates PresignOption effects.
type presignConfig struct {
	contentDisposition string
	contentType        string
}

// PresignAttachment forces the pre-signed GET to return the object as a
// download: Content-Disposition: attachment and Content-Type:
// application/octet-stream. This is the stored-XSS defence for user-uploaded
// blobs — a browser following the URL saves the file instead of rendering an
// uploaded HTML/SVG/JS payload inline in the store's origin.
func PresignAttachment() PresignOption {
	return func(c *presignConfig) {
		c.contentDisposition = "attachment"
		c.contentType = "application/octet-stream"
	}
}

// PresignWantsAttachment reports whether opts include PresignAttachment. It
// lets alternative ObjectStore implementations (test fakes) mirror the real
// S3Store's attachment behaviour without reaching into presignConfig.
func PresignWantsAttachment(opts []PresignOption) bool {
	var cfg presignConfig
	for _, o := range opts {
		o(&cfg)
	}
	return cfg.contentDisposition == "attachment"
}
