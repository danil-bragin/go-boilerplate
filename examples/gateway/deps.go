package gateway

import (
	"context"
	"fmt"
	"net/netip"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/featureflags"
	"go-boilerplate/platform/observability/health"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/storage/blob"

	"go-boilerplate/examples/gateway/internal/sse"
	"go-boilerplate/platform/storage/cache"
	"go-boilerplate/platform/web/ratelimit"

	"github.com/redis/rueidis"
	inmemory "go.openfeature.dev/openfeature/v2/providers/inmemory"
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
	opts := []auth.Option{auth.WithClockSkew(cfg.AuthClockSkew)}
	if cfg.AuthRequiredAZP != "" {
		opts = append(opts, auth.WithRequiredAZP(cfg.AuthRequiredAZP))
	}
	if cfg.AuthAllowInsecureJWKS {
		opts = append(opts, auth.WithAllowInsecureJWKS(true))
	}
	v, err := auth.NewJWKSVerifier(ctx, cfg.JWKSUrl, cfg.JWKSIssuer, cfg.JWKSAudience, opts...)
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

// buildSSE builds the order-status SSE streamer. When REDIS_ADDRS is set a
// dedicated rueidis client carries the status broadcast pub/sub (the cache's
// client is encapsulated inside cache.Cache, and rueidis multiplexes one
// connection per address, so a second client is cheap): the streamer holds
// ONE shared subscription per replica and fans updates out to open streams
// in-process — stream count never adds Redis connections. When Redis is
// unconfigured or unreachable the streamer degrades to polling the
// projection store (sse.Streamer handles a nil client).
func buildSSE(cfg Config, svc *servicekit.Service) *sse.Streamer {
	var client rueidis.Client
	if len(cfg.Cache.RedisAddrs) > 0 && cfg.Cache.RedisAddrs[0] != "" {
		c, err := rueidis.NewClient(cache.BuildRueidisOption(cfg.Cache))
		if err != nil {
			svc.Logger().Warn(
				"gateway: SSE Redis unavailable, falling back to projection-store polling",
				"error", err,
				"redis_addrs", cfg.Cache.RedisAddrs,
			)
		} else {
			client = c
			svc.Closer().Add("sse-redis", func(context.Context) error {
				c.Close()
				return nil
			})
		}
	} else {
		svc.Logger().Info("gateway: REDIS_ADDRS not set — SSE falls back to projection-store polling")
	}
	return sse.New(
		client, svc.Pool(), svc.Logger(), cfg.AuthDisabled,
		sse.WithHeartbeat(cfg.SSEHeartbeat),
		sse.WithPollInterval(cfg.SSEPollInterval),
		sse.WithMaxStreams(cfg.SSEMaxStreams),
	)
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
	flags, err := featureflags.NewInMemory("gateway-"+cfg.HTTP.Addr, map[string]inmemory.InMemoryFlag{
		"order-attachments-enabled": {
			State:          inmemory.Enabled,
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

// ParseTrustedProxies parses a slice of CIDR strings into netip.Prefix values.
// Returns an error immediately on the first invalid CIDR (fail-fast).
func ParseTrustedProxies(cidrs []string) ([]netip.Prefix, error) {
	if len(cidrs) == 0 {
		return nil, nil
	}
	prefixes := make([]netip.Prefix, 0, len(cidrs))
	for _, cidr := range cidrs {
		if cidr == "" {
			continue
		}
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("gateway: invalid TRUSTED_PROXIES CIDR %q: %w", cidr, err)
		}
		prefixes = append(prefixes, p)
	}
	return prefixes, nil
}

// buildLimiter constructs the per-IP rate limiter.
//
// When RatelimitRedis=true and REDIS_ADDRS is configured, a Redis-backed
// distributed limiter is created (info logged). If Redis is unavailable the
// function warns and falls back to in-memory (graceful degradation).
// When RatelimitRedis=false (default) an in-memory limiter is always used.
//
// The in-memory limiter's Close is registered with svc.Closer so the janitor
// goroutine is stopped on shutdown. Redis limiters are stateless (no close
// needed beyond the shared Redis connection managed separately).
func buildLimiter(cfg Config, svc *servicekit.Service) ratelimit.Limiter {
	return newLimiter(cfg, svc, "per-ip", cfg.RatelimitRPS, cfg.RatelimitBurst)
}

// buildAuthedLimiter constructs the SECOND, authed-tier limiter (keyed per
// principal — see httpserver.PrincipalKey). Returns nil when
// RATELIMIT_AUTHED_RPS=0 (the tier is disabled). Memory-vs-Redis selection
// follows the same RATELIMIT_REDIS switch as the per-IP limiter.
func buildAuthedLimiter(cfg Config, svc *servicekit.Service) ratelimit.Limiter {
	if cfg.RatelimitAuthedRPS <= 0 {
		return nil
	}
	return newLimiter(cfg, svc, "authed", cfg.RatelimitAuthedRPS, cfg.RatelimitAuthedBurst)
}

// newLimiter builds one named rate limiter with the given budget: Redis-backed
// when RATELIMIT_REDIS=true and REDIS_ADDRS is reachable, in-memory otherwise
// (graceful degradation, WARN logged). The in-memory janitor is registered
// with svc.Closer under "ratelimit-<name>".
func newLimiter(cfg Config, svc *servicekit.Service, name string, rps float64, burst int) ratelimit.Limiter {
	if cfg.RatelimitRedis && len(cfg.Cache.RedisAddrs) > 0 && cfg.Cache.RedisAddrs[0] != "" {
		// Build a dedicated rueidis client for rate limiting.
		// The cache's rueidis client is encapsulated inside cache.Cache and not
		// accessible via the cqrs.Cache interface, so we open a second connection
		// to the same address. This is cheap: rueidis uses a single multiplexed
		// connection per address.
		client, err := rueidis.NewClient(cache.BuildRueidisOption(cfg.Cache))
		if err != nil {
			svc.Logger().Warn(
				"gateway: rate-limit Redis unavailable, falling back to in-memory limiter",
				"limiter", name,
				"error", err,
				"redis_addrs", cfg.Cache.RedisAddrs,
			)
		} else {
			svc.Logger().Info("gateway: distributed rate limit (Redis)", "limiter", name, "addrs", cfg.Cache.RedisAddrs)
			svc.Closer().Add("ratelimit-redis-"+name, func(context.Context) error {
				client.Close()
				return nil
			})
			// Per-tier key prefix: without it both tiers share the default
			// "rl:" namespace, and an anonymous request (PrincipalKey falls
			// back to the IP key) would double-debit ONE bucket with two
			// different rps/burst clamps — wrong admitted rate and headers.
			return ratelimit.NewRedis(client, rps, burst, ratelimit.WithKeyPrefix("rl:"+name+":"))
		}
	} else if cfg.RatelimitRedis {
		svc.Logger().Warn("gateway: RATELIMIT_REDIS=true but REDIS_ADDRS not set, falling back to in-memory limiter",
			"limiter", name)
	}

	mem := ratelimit.NewMemory(rps, burst)
	svc.Closer().Add("ratelimit-memory-"+name, func(_ context.Context) error {
		mem.Close()
		return nil
	})
	return mem
}
