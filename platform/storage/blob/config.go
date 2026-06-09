package blob

import "go-boilerplate/platform/config"

// Config holds connection parameters for an S3-compatible object store.
// Tags are compatible with github.com/caarlos0/env/v11.
type Config struct {
	// Endpoint is the hostname:port (or just hostname for HTTPS) of the store.
	// Leave empty to use the default AWS S3 endpoint for Region.
	Endpoint string `env:"S3_ENDPOINT" envDefault:"localhost:8333"`
	// AccessKey is the access-key ID.
	AccessKey string `env:"S3_ACCESS_KEY"`
	// SecretKey is the secret access key. config.Secret redacts it from every
	// print/log path; the s3 client constructor calls Reveal() explicitly.
	SecretKey config.Secret `env:"S3_SECRET_KEY"`
	// Bucket is the bucket that this store operates on.
	Bucket string `env:"S3_BUCKET" envDefault:"app"`
	// UseSSL controls whether TLS is used for the connection.
	UseSSL bool `env:"S3_USE_SSL" envDefault:"false"`
	// Region is the AWS region of the bucket.
	Region string `env:"S3_REGION" envDefault:"us-east-1"`
	// UsePathStyle forces path-style addressing (http://host:port/bucket/key)
	// instead of virtual-hosted style (http://bucket.host/key). Required for
	// local S3-compatible stores (SeaweedFS, LocalStack); disable for AWS S3.
	UsePathStyle bool `env:"S3_USE_PATH_STYLE" envDefault:"true"`
}

// endpointURL renders Endpoint as a full URL with the scheme implied by UseSSL.
func (c Config) endpointURL() string {
	scheme := "http://"
	if c.UseSSL {
		scheme = "https://"
	}
	return scheme + c.Endpoint
}
