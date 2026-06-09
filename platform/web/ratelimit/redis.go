package ratelimit

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/rueidis"
)

// luaTokenBucket is an atomic token-bucket Lua script.
//
// KEYS[1] = bucket hash key
// ARGV[1] = rps          (float, tokens per second)
// ARGV[2] = burst        (int, maximum token capacity)
// ARGV[3] = idle_ttl_ms  (int, key TTL in milliseconds)
//
// now_ms is derived from Redis TIME so all replicas share the same clock,
// eliminating over-admission caused by wall-clock skew between app instances.
//
// Scripts that call TIME (a non-deterministic command) and then write are safe
// on Redis ≥7 because replication uses effects-based (AOF) propagation — the
// written values, not the script itself, are replicated. rueidis targets Redis
// ≥7, so this is always correct.
//
// The script is a single Redis command — it is atomic on a single Redis node
// and on cluster when KEYS[1] hashes to the same slot.
//
//nolint:gosec // G101: luaTokenBucket contains Redis field names ('t','ts'), not credentials.
const luaTokenBucket = `
local t      = redis.call('TIME')
local now    = t[1] * 1000 + math.floor(t[2] / 1000)
local tokens = tonumber(redis.call('HGET', KEYS[1], 't') or ARGV[2])
local ts     = tonumber(redis.call('HGET', KEYS[1], 'ts') or now)
local refill = (now - ts) / 1000.0 * tonumber(ARGV[1])
tokens = math.min(tokens + refill, tonumber(ARGV[2]))
local allowed = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
redis.call('HSET', KEYS[1], 't', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return allowed
`

var bucketScript = rueidis.NewLuaScript(luaTokenBucket)

// Redis is a distributed, per-key rate limiter backed by a Redis hash and an
// atomic Lua token bucket. It is safe for concurrent use from multiple
// goroutines and is designed to work across multiple application replicas
// sharing a single Redis instance (or cluster).
type Redis struct {
	client    rueidis.Client
	rps       float64
	burst     int
	idleTTLms string // pre-formatted for Lua ARGV
	failOpen  bool
	onError   func(error)
	prefix    string
}

// RedisOption configures a Redis rate limiter.
type RedisOption func(*Redis)

// WithFailClosed makes Allow return (false, err) on Redis errors instead of
// the default fail-open behaviour ((true, nil)).
//
// Tradeoff: fail-open preserves edge availability when Redis is degraded but
// may allow bursts beyond the configured rate. WithFailClosed gives strict
// enforcement at the cost of denying all requests when Redis is unavailable.
func WithFailClosed() RedisOption {
	return func(r *Redis) { r.failOpen = false }
}

// WithOnError registers a callback that is invoked whenever a Redis error
// occurs inside Allow. It is called from the Allow goroutine; keep it
// non-blocking (e.g. increment a metric counter).
func WithOnError(f func(error)) RedisOption {
	return func(r *Redis) { r.onError = f }
}

// WithKeyPrefix sets the Redis key prefix prepended to every bucket key
// (default "rl:"). Use this to namespace limiters in a shared Redis instance.
func WithKeyPrefix(p string) RedisOption {
	return func(r *Redis) { r.prefix = p }
}

// NewRedis constructs a Redis rate limiter.
// rps is the sustained token refill rate; burst is the maximum bucket depth.
//
// The key TTL is set to 2× max(burst/rps, 60s) so that idle buckets expire
// automatically and Redis memory is bounded. The factor of two ensures a
// briefly-idle bucket is not evicted mid-window.
func NewRedis(client rueidis.Client, rps float64, burst int, opts ...RedisOption) *Redis {
	// idle_ttl_ms = 2 × max(burst/rps seconds, 60s), expressed in milliseconds.
	// The max ensures a minimum TTL of 120s for very high-rps limiters.
	idleSec := float64(burst) / rps
	if idleSec < 60 {
		idleSec = 60
	}
	idleTTLms := int64(idleSec * 2 * 1000)

	r := &Redis{
		client:    client,
		rps:       rps,
		burst:     burst,
		idleTTLms: strconv.FormatInt(idleTTLms, 10),
		failOpen:  true,
		onError:   func(error) {},
		prefix:    "rl:",
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Allow returns (true, nil) if key is allowed, (false, nil) if rate-limited.
// On Redis error the behaviour depends on the fail-open / fail-closed setting:
//   - fail-open (default): returns (true, nil) and calls onError(err).
//   - fail-closed: returns (false, err) and calls onError(err).
func (r *Redis) Allow(ctx context.Context, key string) (bool, error) {
	rpsStr := strconv.FormatFloat(r.rps, 'f', -1, 64)
	burstStr := strconv.Itoa(r.burst)
	bucketKey := r.prefix + key

	// now_ms is no longer passed as ARGV — the Lua script calls Redis TIME
	// internally to avoid clock skew between application replicas.
	res := bucketScript.Exec(
		ctx, r.client,
		[]string{bucketKey},
		[]string{rpsStr, burstStr, r.idleTTLms},
	)
	if err := res.Error(); err != nil {
		r.onError(fmt.Errorf("ratelimit: redis: %w", err))
		if r.failOpen {
			return true, nil
		}
		return false, fmt.Errorf("ratelimit: redis: %w", err)
	}

	allowed, err := res.AsInt64()
	if err != nil {
		r.onError(fmt.Errorf("ratelimit: redis parse: %w", err))
		if r.failOpen {
			return true, nil
		}
		return false, fmt.Errorf("ratelimit: redis parse: %w", err)
	}
	return allowed == 1, nil
}
