package blob

// Config holds connection parameters for an S3-compatible object store.
// Tags are compatible with github.com/caarlos0/env/v11.
type Config struct {
	// Endpoint is the hostname:port (or just hostname for HTTPS) of the store.
	Endpoint string `env:"S3_ENDPOINT" envDefault:"localhost:9000"`
	// AccessKey is the access-key ID.
	AccessKey string `env:"S3_ACCESS_KEY"`
	// SecretKey is the secret access key.
	SecretKey string `env:"S3_SECRET_KEY"`
	// Bucket is the bucket that this store operates on.
	Bucket string `env:"S3_BUCKET" envDefault:"app"`
	// UseSSL controls whether TLS is used for the connection.
	UseSSL bool `env:"S3_USE_SSL" envDefault:"false"`
	// Region is the AWS/MinIO region of the bucket.
	Region string `env:"S3_REGION" envDefault:"us-east-1"`
}
