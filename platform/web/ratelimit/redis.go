package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

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
local rps    = tonumber(ARGV[1])
local tokens = tonumber(redis.call('HGET', KEYS[1], 't') or ARGV[2])
local ts     = tonumber(redis.call('HGET', KEYS[1], 'ts') or now)
local refill = (now - ts) / 1000.0 * rps
tokens = math.min(tokens + refill, tonumber(ARGV[2]))
local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
elseif rps > 0 then
  retry_ms = math.ceil((1 - tokens) / rps * 1000)
end
local reset_ms = 0
if rps > 0 and tokens < tonumber(ARGV[2]) then
  reset_ms = math.ceil((tonumber(ARGV[2]) - tokens) / rps * 1000)
end
redis.call('HSET', KEYS[1], 't', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return {allowed, math.floor(tokens), retry_ms, reset_ms}
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

// Allow reports whether key may proceed, together with the remaining budget,
// the time until the bucket is full (Reset) and (when denied) the wait until
// the next token, all computed atomically by the Lua bucket. On Redis error
// the behaviour depends on the fail-open / fail-closed setting:
//   - fail-open (default): returns an Allowed result with Remaining=-1
//     (unknown budget, Reset=0) and calls onError(err).
//   - fail-closed: returns a denied result and the error; calls onError(err).
func (r *Redis) Allow(ctx context.Context, key string) (Result, error) {
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
		return r.errResult(fmt.Errorf("ratelimit: redis: %w", err))
	}

	arr, err := res.AsIntSlice()
	if err != nil || len(arr) != 4 {
		if err == nil {
			err = fmt.Errorf("unexpected reply length %d", len(arr))
		}
		return r.errResult(fmt.Errorf("ratelimit: redis parse: %w", err))
	}

	out := Result{
		Allowed:   arr[0] == 1,
		Limit:     int64(r.burst),
		Remaining: arr[1],
		Reset:     time.Duration(arr[3]) * time.Millisecond,
	}
	if out.Remaining < 0 {
		out.Remaining = 0
	}
	if !out.Allowed {
		out.RetryAfter = time.Duration(arr[2]) * time.Millisecond
	}
	return out, nil
}

// errResult maps a Redis failure to the configured fail-open / fail-closed
// behaviour and reports it via onError.
func (r *Redis) errResult(err error) (Result, error) {
	r.onError(err)
	if r.failOpen {
		// Allowed but with unknown remaining budget.
		return Result{Allowed: true, Limit: int64(r.burst), Remaining: -1}, nil
	}
	return Result{Limit: int64(r.burst), Remaining: -1}, err
}
