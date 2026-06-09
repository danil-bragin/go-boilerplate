package gateway

import (
	"go-boilerplate/examples/servicekit"
	"go-boilerplate/platform/storage/blob"
	"go-boilerplate/platform/storage/cache"
	"go-boilerplate/platform/web/httpserver"
)

// Config aggregates all configuration for the gateway service.
type Config struct {
	servicekit.Config
	HTTP                httpserver.Config
	Cache               cache.Config
	S3                  blob.Config
	CommandsTopic       string `env:"GATEWAY_COMMANDS_TOPIC"        envDefault:"orders.commands"`
	OrdersEventsTopic   string `env:"GATEWAY_ORDERS_EVENTS_TOPIC"   envDefault:"orders.events"`
	PaymentsEventsTopic string `env:"GATEWAY_PAYMENTS_EVENTS_TOPIC" envDefault:"payments.events"`
	AuthDisabled        bool   `env:"GATEWAY_AUTH_DISABLED"         envDefault:"false"`
	JWKSUrl             string `env:"GATEWAY_JWKS_URL"              envDefault:""`
	JWKSIssuer          string `env:"GATEWAY_JWKS_ISSUER"           envDefault:""`
	JWKSAudience        string `env:"GATEWAY_JWKS_AUDIENCE"         envDefault:""`
	// CORSOrigins is the list of allowed CORS origins for the public HTTP server.
	// Use ["*"] for dev/demo. In production set explicit origins.
	// Default "*" allows any origin (demo-safe; auth should enforce identity).
	CORSOrigins []string `env:"GATEWAY_CORS_ORIGINS"          envSeparator:"," envDefault:"*"`
}
