# Cross-cutting (Sub-project 5) Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Integration-heavy where noted; verify lib APIs with `go doc` and adapt. Docker required for cache/blob tests.

**Goal:** The reusable cross-cutting platform packages: two-tier cache, object storage, resilience policies, OIDC/JWT auth, RBAC authorization, audit, and feature flags. Each is independent and slots into the CQRS pipeline (SP4) or HTTP edge (SP1) where relevant.

**Architecture:** All under `platform/`. `cache` implements the `cqrs.Cache` interface (L1 otter + L2 rueidis + singleflight + TTL jitter). `blob` is an `ObjectStore` interface backed by minio-go (works against MinIO + S3). `resilience` wraps failsafe-go into small policy builders. `auth` validates RS256 JWTs against a JWKS (Keycloak in prod; httptest JWKS in tests) and exposes a pluggable `httpserver` middleware + ctx principal. `authz` is a `cqrs.Behavior` enforcing role/permission policy from the principal. `audit` is a `cqrs.Behavior` recording command executions to an audit store. `featureflags` wraps OpenFeature with an in-memory provider.

**Tech Stack:** `github.com/maypok86/otter/v2` · `github.com/redis/rueidis` · `golang.org/x/sync/singleflight` · `github.com/minio/minio-go/v7` + testcontainers minio · `github.com/failsafe-go/failsafe-go` · `github.com/lestrrat-go/jwx/v2` (jwt+jwk) · `github.com/open-feature/go-sdk`. Tests: testcontainers (redis, minio), httptest (JWKS).

**Depends on:** SP1 (log, run, httpserver, httpx), SP4 (cqrs.Cache, cqrs.Behavior), SP2 (pg for audit store).

---

## Task 1 — `platform/cache` (two-tier: otter L1 + rueidis L2 + singleflight)
- `Config{ RedisAddrs []string \`env:"REDIS_ADDRS" envSeparator:","\`; L1Capacity int; DefaultTTL time.Duration; TTLJitter float64 }`.
- `Cache` struct implementing `cqrs.Cache` (`Get(ctx,key)([]byte,bool)`, `Set(ctx,key,val,ttl)`), plus `GetOrLoad(ctx, key, ttl, loader func(ctx)([]byte,error)) ([]byte, error)` with **singleflight** (collapse concurrent misses) and **TTL jitter** (±jitter%).
- L1 = otter v2 cache (`otter.Must[string,[]byte](&otter.Options{MaximumSize:...})` — verify v2 API via `go doc github.com/maypok86/otter/v2`); L2 = rueidis client (`rueidis.NewClient(rueidis.ClientOption{InitAddress:addrs})`), use client-side caching (`DoCache`) for L2 reads where available.
- Flow: Get → L1 hit? return; else L2 (rueidis) hit? populate L1, return; else miss. Set → write L2 + L1, jittered TTL. GetOrLoad → Get; on miss singleflight(key) → loader → Set → return.
- `Close(ctx) error` closes rueidis; `HealthCheck(ctx)` pings redis. Register with run.Closer/health by caller.
- Tests (testcontainers redis): round-trip via L2; L1 serves after first L2 hit; GetOrLoad collapses N concurrent misses to ONE loader call (atomic counter, errgroup of 20); TTL jitter within bounds. Also a pure-L1 unit test without redis if the struct supports `RedisAddrs` empty → L1-only mode (optional; if simpler require redis, document).
- **Verify** otter v2 + rueidis APIs. Commit `feat(cache): two-tier otter L1 + rueidis L2 with singleflight and TTL jitter`.

## Task 2 — `platform/blob` (ObjectStore + minio-go)
- `ObjectStore interface { Put(ctx, key string, r io.Reader, size int64, contentType string) error; Get(ctx, key) (io.ReadCloser, error); Delete(ctx, key) error; Exists(ctx, key)(bool,error); PresignGet(ctx, key, ttl)(string,error); List(ctx, prefix)([]string,error) }`.
- `Config{ Endpoint, AccessKey, SecretKey, Bucket string; UseSSL bool; Region string }` (caarlos0/env tags).
- `MinioStore` implementing it via `minio.New(endpoint, &minio.Options{Creds:..., Secure:UseSSL})`; `New(cfg)` ensures the bucket exists (`MakeBucket` / `BucketExists`). `HealthCheck` via `BucketExists`.
- Tests (testcontainers minio — `testcontainers-go/modules/minio`): Put→Get round-trip (bytes equal, content-type), Exists true/false, Delete, List by prefix, PresignGet returns a URL that actually downloads the object (http GET the presigned URL). Verify minio module constructor + creds accessor.
- Commit `feat(blob): ObjectStore interface with MinIO/S3 implementation`.

## Task 3 — `platform/resilience` (failsafe-go policy builders)
- Thin builders returning failsafe policies + an `Execute` helper:
  - `Retry(maxAttempts int, baseDelay time.Duration) retrypolicy.RetryPolicy[any]` with exponential + full jitter.
  - `CircuitBreaker(failureThreshold uint, delay time.Duration) circuitbreaker.CircuitBreaker[any]`.
  - `Timeout(d time.Duration) timeout.Timeout[any]`.
  - `Bulkhead(maxConcurrent int) bulkhead.Bulkhead[any]` (verify failsafe bulkhead API; if absent use `x/sync/semaphore` wrapper).
  - `Do(ctx, fn func(ctx)(error), policies...) error` and `Get[T](ctx, fn func(ctx)(T,error), policies...) (T,error)` wrapping `failsafe.NewExecutor`/`failsafe.With`.
- Also a distributed rate limiter helper note (redis_rate) deferred — in-proc `golang.org/x/time/rate` wrapper `RateLimiter(rps, burst)` + `Allow`/`Wait`.
- Tests (pure-Go, no containers): Retry retries N then succeeds/gives up; CircuitBreaker opens after threshold then rejects; Timeout cancels a slow fn; Bulkhead caps concurrency; full-jitter present. Verify failsafe-go generic API via `go doc github.com/failsafe-go/failsafe-go`.
- Commit `feat(resilience): failsafe-go retry/circuit-breaker/timeout/bulkhead + rate limiter`.

## Task 4 — `platform/auth` (OIDC/JWT + pluggable middleware)
- `Principal struct { Subject string; Username string; Roles []string; Claims map[string]any }`; ctx helpers `Into`/`From`.
- `Verifier interface { Verify(ctx, rawToken string) (Principal, error) }`.
- `JWKSVerifier` using `github.com/lestrrat-go/jwx/v2/jwk` (cached JWKS via `jwk.NewCache`/`jwk.Fetch`) + `jwt.Parse` with `jwt.WithKeySet`, validating issuer/audience/exp. Constructor `NewJWKSVerifier(ctx, jwksURL, issuer, audience string) (*JWKSVerifier, error)`. Extract roles from a configurable claim (Keycloak puts them in `realm_access.roles`); provide `WithRolesClaim(path)`.
- Middleware `Middleware(v Verifier) func(http.Handler) http.Handler` for `platform/httpserver`: reads `Authorization: Bearer <jwt>`, verifies, puts Principal in ctx, 401 problem+json on failure. `RequireAuth` variant. Pluggable: accepts any `Verifier` so the IdP is swappable.
- Tests (no Keycloak; httptest JWKS): generate an RSA key, build a JWKS handler serving the public key, sign a JWT (lestrrat jwt) with iss/aud/exp/roles, run `JWKSVerifier.Verify` → Principal populated; expired/ wrong-issuer/ wrong-aud/ bad-signature → error; middleware: valid bearer → 200 + principal in ctx; missing/invalid → 401 problem+json. Verify jwx v2 API.
- Commit `feat(auth): JWKS/OIDC JWT verifier and pluggable HTTP auth middleware`.

## Task 5 — `platform/authz` (RBAC behavior)
- `Policy interface { Authorize(p auth.Principal, action string) error }`; a simple `RBAC` impl: `action → required roles` map; pass if principal has any required role; else `ErrForbidden`.
- `Require[C,R](policy Policy, action string) cqrs.Behavior[C,R]` — pulls `auth.Principal` from ctx (via `auth.From`); if absent → `ErrUnauthenticated`; else `policy.Authorize(principal, action)`; on deny return zero,err; else next.
- Tests (pure-Go): principal with role passes; without role → ErrForbidden; no principal → ErrUnauthenticated; behavior composes with Decorate.
- Commit `feat(authz): RBAC policy + CQRS authorization behavior`.

## Task 6 — `platform/audit` (audit behavior + store)
- `Entry struct { Actor string; Action string; Subject string; At time.Time; Metadata map[string]string }`.
- `Store interface { Record(ctx, Entry) error }`. `PgStore` writing to an `audit_log` table (migration `migrations/00001_audit.sql`), using `pg.FromContext` so the audit write joins the command tx (atomic with the business write). 
- `Audit[C,R](store Store, action string, subjectFor func(C) string) cqrs.Behavior[C,R]` — after a successful command, records an Entry (actor from `auth.From(ctx)` principal, subject from subjectFor); on command error, do NOT record (or record failure — choose: record only success for SP5, document). Runs INSIDE the command tx (so audit + business write commit together) — note ordering: Audit behavior should be inside Transaction in the pipeline.
- Tests (pg testcontainers): a command wrapped with Transaction+Audit records an audit row on success; on command error the audit row is rolled back too (atomicity). Verify actor extraction.
- Commit `feat(audit): audit-log store and CQRS audit behavior (tx-atomic)`.

## Task 7 — `platform/featureflags` (OpenFeature)
- Wrap `github.com/open-feature/go-sdk/openfeature`: `Provider` setup with an in-memory provider for the example; `Flags struct { client *openfeature.Client }` with `BoolFlag(ctx, key, default) bool`, `StringFlag`, `IntFlag`. `NewInMemory(map[string]bool/...)` for tests/examples; document swapping the provider (flagd/LaunchDarkly) in prod.
- Tests (pure-Go): in-memory provider returns configured flag values; default on unknown key; per-context targeting optional. Verify openfeature go-sdk API (`go doc github.com/open-feature/go-sdk/openfeature`).
- Commit `feat(featureflags): OpenFeature wrapper with in-memory provider`.

## Task 8 — verify + review
- Whole-repo `go build`, `go test -race ./...`, vet/fmt/lint, coverage. Confirm package boundaries (authz/audit import auth+cqrs; cache implements cqrs.Cache; nothing imports cmd/examples).

---
## Notes / deferred
- Distributed rate limiting (redis_rate) + adaptive load-shedding → wire when needed.
- Real Keycloak + flagd + Redis cluster → compose in SP7.
- Wiring these into services (registering closers/health, applying middleware/behaviors) → SP6 examples.
