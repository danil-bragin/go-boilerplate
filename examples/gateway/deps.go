package gateway

import (
	"context"
	"fmt"

	"go-boilerplate/examples/servicekit"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/featureflags"
	"go-boilerplate/platform/observability/health"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/blob"
	"go-boilerplate/platform/storage/cache"

	"github.com/open-feature/go-sdk/openfeature/memprovider"
)

// buildVerifier resolves the auth.Verifier for NewApp.
// Returns (nil, nil) when auth is disabled — callers must check cfg.AuthDisabled.
// Returns an error when auth is enabled and no verifier can be constructed
// (fail-closed: the service refuses to start).
//
// The caller (NewApp) only uses the verifier when !cfg.AuthDisabled, so a nil
// return value when auth is disabled is intentional and safe.
func buildVerifier(ctx context.Context, cfg Config, override auth.Verifier) (auth.Verifier, error) {
	if cfg.AuthDisabled {
		return override, nil // override may itself be nil — fine when auth is off
	}
	if override != nil {
		return override, nil
	}
	v, err := auth.NewJWKSVerifier(ctx, cfg.JWKSUrl, cfg.JWKSIssuer, cfg.JWKSAudience)
	if err != nil {
		return nil, fmt.Errorf("gateway: building JWKS verifier (auth is enabled): %w", err)
	}
	return v, nil
}

// buildCache tries to build a Redis-backed two-tier cache.
// Returns nil when Redis is unconfigured or unreachable — graceful degradation.
func buildCache(cfg Config, svc *servicekit.Service) cqrs.Cache {
	if len(cfg.Cache.RedisAddrs) == 0 || cfg.Cache.RedisAddrs[0] == "" {
		svc.Logger().Info("gateway: REDIS_ADDRS not set, starting without cache")
		return nil
	}
	c, err := cache.New(cfg.Cache)
	if err != nil {
		svc.Logger().Warn(
			"gateway: cache unavailable, starting without Redis caching",
			"error", err,
			"redis_addrs", cfg.Cache.RedisAddrs,
		)
		return nil
	}
	svc.Closer().Add("cache", func(ctx context.Context) error {
		return c.Close(ctx)
	})
	svc.Health().AddReadiness("cache", health.Check(func(ctx context.Context) error {
		return c.HealthCheck(ctx)
	}))
	return c
}

// buildBlob tries to build a MinIO-backed object store for order attachments.
// Returns nil when the S3 endpoint is unconfigured or unreachable — graceful degradation.
func buildBlob(ctx context.Context, cfg Config, svc *servicekit.Service) blob.ObjectStore {
	if cfg.S3.Endpoint == "" {
		svc.Logger().Info("gateway: S3_ENDPOINT not set, starting without blob/attachments")
		return nil
	}
	s, err := blob.New(ctx, cfg.S3)
	if err != nil {
		svc.Logger().Warn(
			"gateway: blob/attachments disabled (S3 unavailable)",
			"error", err,
			"endpoint", cfg.S3.Endpoint,
		)
		return nil
	}
	return s
}

// buildFeatureFlags builds the in-memory feature-flags provider seeded with
// the order-attachments flag. Returns nil on error (graceful degradation).
func buildFeatureFlags(cfg Config, svc *servicekit.Service) *featureflags.Flags {
	flags, err := featureflags.NewInMemory("gateway-"+cfg.HTTP.Addr, map[string]memprovider.InMemoryFlag{
		"order-attachments-enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "on",
			Variants:       map[string]any{"on": true, "off": false},
		},
	})
	if err != nil {
		svc.Logger().Warn("gateway: feature flags unavailable, attachments will be disabled", "error", err)
		return nil
	}
	return flags
}
